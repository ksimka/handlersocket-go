/*
Copyright 2011 Brian Ketelsen

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

*/

package handlersocket

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"strconv"
	"strings"
	"sync"
)

const (
	DefaultReadPort  = 9998
	DefaultWritePort = 9999
)

/**
 * The main HandlerSocket struct
 * shamelessly modeled after Philio/GoMySQL
 * for consistency of usage
 */
type HandlerSocket struct {
	auth        Auth
	rdConn      net.Conn
	wrConn      net.Conn
	rdConnected bool
	wrConnected bool
	ctx         context.Context

	indexes map[int][]string
	sync.RWMutex

	respBody bytes.Buffer
}

type Auth struct {
	host      string
	readPort  int
	writePort int
}

type Row struct {
	Data map[string]interface{}
}

type hsopencommand struct {
	command string
	params  []string
}

type hsfindcommand struct {
	command string
	params  []string
	limit   int
	offset  int
}

type hsmodifycommand struct {
	command  string
	criteria []string
	limit    int
	offset   int
	mop      string
	newvals  []string
}

type hsinsertcommand struct {
	command string
	params  []string
}

type Response struct {
	ReturnCode string
	Data       []string
}

func (hs *HandlerSocket) OpenIndex(index int, dbName string, tableName string, indexName string, columns ...string) (err error) {
	if !hs.rdConnected || !hs.wrConnected {
		err = hs.connect()
		if err != nil {
			return fmt.Errorf("error connecting for open index command: %w", err)
		}
	}

	cols := strings.Join(columns, ",")
	strindex := strconv.Itoa(index)
	a := []string{strindex, dbName, tableName, indexName, cols}

	log.Println("open index", cols, strindex, a)

	// Create a new open command
	openCmd := &hsopencommand{command: "P", params: a}

	// Write to the read connection
	err = openCmd.write(hs.rdConn)
	if err != nil {
		return fmt.Errorf("error writing OpenIndex to read connection: %w", err)
	}

	// Write to the write connection
	err = openCmd.write(hs.wrConn)
	if err != nil {
		return fmt.Errorf("error writing OpenIndex to write connection: %w", err)
	}

	log.Println("reading response...")

	// Read from the read connection
	rdMessage, err := hs.readResponse(hs.rdConn)
	if err != nil {
		return fmt.Errorf("error reading OpenIndex from read connection: %w", err)
	}

	// Read from the write connection
	wrMessage, err := hs.readResponse(hs.wrConn)
	if err != nil {
		return fmt.Errorf("error reading OpenIndex from write connection: %w", err)
	}

	log.Println("response", rdMessage, wrMessage)

	hs.Lock()
	hs.indexes[index] = columns
	hs.Unlock()

	if rdMessage.ReturnCode != "0" {
		return fmt.Errorf("error opening index '%d' on read connection", index)
	}

	if wrMessage.ReturnCode != "0" {
		return fmt.Errorf("error opening index '%d' on write connection", index)
	}

	return nil
}

/*
----------------------------------------------------------------------------
Updating/Deleting data

The 'find_modify' request has the following syntax.

		<indexid> <op> <vlen> <v1> ... <vn> <limit> <offset> <mop> <m1> ... <mk>

	  - <mop> is either 'U' (update) or 'D' (delete).
	  - <m1> ... <mk> specifies the column values to set. The length of <m1> ...
	    <mk> must be smaller than or equal to the length of <columns> specified by
	    the corresponding 'open_index' request. If <mop> is 'D', these parameters
	    are ignored.

ind op	pc	key	lim off	mop	newpk	newval ...
1	=	1	red	1	0	U	red	brian
----------------------------------------------------------------------------
*/
func (hs *HandlerSocket) Modify(index int, oper string, limit int, offset int, modifyOper string, keys []string, newvals []string) (modifiedRows int, err error) {
	if modifyOper != "D" && modifyOper != "U" {
		return 0, fmt.Errorf("invalid modify operation: %s", modifyOper)
	}

	query := strings.Join(keys, "\t")
	queryCount := strconv.Itoa(len(keys))

	a := []string{oper, queryCount, query}

	var modifyCmd *hsmodifycommand

	if modifyOper == "D" {
		modifyCmd = &hsmodifycommand{command: strconv.Itoa(index), criteria: a, limit: limit, offset: offset, mop: modifyOper}
	}

	if modifyOper == "U" {
		modifyCmd = &hsmodifycommand{command: strconv.Itoa(index), criteria: a, limit: limit, offset: offset, mop: modifyOper, newvals: newvals}
	}

	// Write to the write connection
	err = modifyCmd.write(hs.wrConn)
	if err != nil {
		return 0, fmt.Errorf("error writing Modify command: %w", err)
	}

	// Read from the write connection
	message, err := hs.readResponse(hs.wrConn)
	if err != nil {
		return 0, fmt.Errorf("error reading response for Modify command: %w", err)
	}

	if message.ReturnCode == "1" {
		return 0, fmt.Errorf("error modifying data: %w", err)
	}

	return strconv.Atoi(strings.TrimSpace(message.Data[1]))

}

func (hs *HandlerSocket) Find(index int, oper string, limit int, offset int, vals ...string) (rows []Row, err error) {
	cols := strings.Join(vals, "\t")
	strindex := strconv.Itoa(index)
	colCount := strconv.Itoa(len(vals))
	a := []string{oper, colCount, cols}

	// Create a new find command
	findCmd := &hsfindcommand{command: strindex, params: a, limit: limit, offset: offset}

	// Write to the read connection
	err = findCmd.write(hs.rdConn)
	if err != nil {
		return nil, fmt.Errorf("error writing Find command: %w", err)
	}

	// Read from the read connection
	message, err := hs.readResponse(hs.rdConn)
	if err != nil {
		return nil, fmt.Errorf("error reading Find command: %w", err)
	}

	return hs.parseFindResult(index, message)

}

/*
----------------------------------------------------------------------------
Inserting data

The 'insert' request has the following syntax.

		<indexid> '+' <vlen> <v1> ... <vn>

	  - <vlen> indicates the length of the trailing parameters <v1> ... <vn>. This
	    must be smaller than or equal to the length of <columns> specified by the
	    corresponding 'open_index' request.
	  - <v1> ... <vn> specify the column values to set. For columns not in
	    <columns>, the default values for each column are set.

----------------------------------------------------------------------------
*/
func (hs *HandlerSocket) Insert(index int, vals ...string) (err error) {
	cols := strings.Join(vals, "\t")
	strindex := strconv.Itoa(index)
	colCount := strconv.Itoa(len(vals))
	oper := "+"

	a := []string{oper, colCount, cols}

	// Create a new insert command
	insertCmd := &hsinsertcommand{command: strindex, params: a}

	// Write to the write connection
	err = insertCmd.write(hs.wrConn)
	if err != nil {
		return fmt.Errorf("error writing Insert command: %w", err)
	}

	// Read from the write connection
	message, err := hs.readResponse(hs.wrConn)
	if err != nil {
		return fmt.Errorf("error reading response for Insert command: %w", err)
	}

	if message.ReturnCode == "1" {
		return fmt.Errorf("data exists, ret code '1'")
	}

	if message.ReturnCode != "0" {
		return fmt.Errorf("error inserting data, ret code '%s'", message.ReturnCode)
	}

	return nil
}

func (hs *HandlerSocket) parseFindResult(index int, hsr Response) (rows []Row, err error) {
	fieldCount, err := strconv.Atoi(hsr.Data[0])
	if err != nil {
		return nil, fmt.Errorf("error parsing field count: %w", err)
	}

	hs.RLock()
	colNames := hs.indexes[index]
	hs.RUnlock()

	remainingFields := len(hsr.Data) - 1
	if fieldCount > 0 {
		rowsCount := remainingFields / fieldCount
		rows = make([]Row, rowsCount)

		offset := 1

		for r := 0; r < rowsCount; r++ {
			d := make(map[string]interface{}, fieldCount)
			for f := 0; f < fieldCount; f++ {
				d[colNames[f]] = hsr.Data[offset+f]
			}

			rows[r] = Row{Data: d}
			offset += fieldCount
		}
	}

	return rows, nil
}

/**
 * Close the connection to the server
 */
func (hs *HandlerSocket) Close() error {
	if !hs.rdConnected || !hs.wrConnected {
		return errors.New("not connected")
	}

	errRd := hs.rdConn.Close()
	hs.rdConnected = false

	errWr := hs.wrConn.Close()
	hs.wrConnected = false

	if errRd != nil || errWr != nil {
		return fmt.Errorf("error closing connections: read=%s, write=%s", errRd, errWr)
	}

	return nil
}

/**
 * Reconnect (if connection droppped etc)
 */
func (hs *HandlerSocket) Reconnect() (err error) {
	if hs.rdConnected {
		_ = hs.rdConn.Close()
		hs.rdConnected = false
	}

	if hs.wrConnected {
		_ = hs.wrConn.Close()
		hs.wrConnected = false
	}

	return hs.connect()
}

/**
 * Connect to a server
 */
func (hs *HandlerSocket) Connect(params ...interface{}) (err error) {
	if hs.rdConnected || hs.wrConnected {
		return errors.New("already connected")
	}

	// Check min number of params
	if len(params) < 1 {
		err = errors.New("hostname is required")
		return
	}

	// Parse params
	hs.parseParams(params)

	return hs.connect()
}

/**
 * Create a new instance of the package
 */
func New(ctx context.Context) (hs *HandlerSocket) {
	// Create and return a new instance of HandlerSocket
	hs = new(HandlerSocket)
	hs.ctx = ctx

	return hs
}

/**
 * Create connection to server using unix socket or tcp/ip then setup buffered reader/writer
 */
func (hs *HandlerSocket) connect() (err error) {
	if hs.rdConnected || hs.wrConnected {
		return errors.New("already connected")
	}

	targetRdAddress := fmt.Sprintf("%s:%d", hs.auth.host, hs.auth.readPort)
	wrTargetAddress := fmt.Sprintf("%s:%d", hs.auth.host, hs.auth.writePort)

	hsRdAddress, err := net.ResolveTCPAddr("tcp", targetRdAddress)
	hsWrAddress, err := net.ResolveTCPAddr("tcp", wrTargetAddress)

	log.Println("connect", targetRdAddress, wrTargetAddress, hsRdAddress, hsWrAddress)

	var dialer net.Dialer

	hs.rdConn, err = dialer.DialContext(hs.ctx, "tcp", hsRdAddress.String())
	if err != nil {
		return fmt.Errorf("error connecting to read port: %w", err)
	}
	hs.rdConnected = true

	hs.wrConn, err = dialer.DialContext(hs.ctx, "tcp", hsWrAddress.String())
	if err != nil {
		return fmt.Errorf("error connecting to write port: %w", err)
	}
	hs.wrConnected = true

	hs.Lock()
	hs.indexes = make(map[int][]string, 10)
	hs.Unlock()

	log.Println("connected")

	return nil
}

/**
 * Parse params given to Connect()
 */
func (hs *HandlerSocket) parseParams(p []interface{}) {
	// Assign default values
	hs.auth.readPort = DefaultReadPort
	hs.auth.writePort = DefaultWritePort
	// Host / username are required
	hs.auth.host = p[0].(string)
	if len(p) > 1 {
		hs.auth.readPort = p[1].(int)
	}
	if len(p) > 2 {
		hs.auth.writePort = p[2].(int)
	}

	return
}

func (f *hsopencommand) write(w io.Writer) error {
	_, err := fmt.Fprintf(w, "%s\t%s\n", f.command, strings.Join(f.params, "\t"))

	return err
}

func (f *hsfindcommand) write(w io.Writer) error {
	_, err := fmt.Fprintf(w, "%s\t%s\t%d\t%d\n", f.command, strings.Join(f.params, "\t"), f.limit, f.offset)

	return err
}

func (f *hsmodifycommand) write(w io.Writer) error {
	_, err := fmt.Fprintf(w, "%s\t%s\t%d\t%d\t%s\t%s\n", f.command, strings.Join(f.criteria, "\t"), f.limit, f.offset, f.mop, strings.Join(f.newvals, "\t"))

	return err
}

func (f *hsinsertcommand) write(w io.Writer) error {
	_, err := fmt.Fprintf(w, "%s\t%s\n", f.command, strings.Join(f.params, "\t"))

	return err
}

func (hs *HandlerSocket) readResponse(conn net.Conn) (Response, error) {
	r := bufio.NewReader(conn)

	for {
		b, err := r.ReadByte()
		if err != nil {
			if err == io.EOF {
				break
			}

			return Response{}, fmt.Errorf("error reading response: %w", err)
		}

		if b == '\n' {
			break
		}

		err = hs.respBody.WriteByte(b)
		if err != nil {
			return Response{}, fmt.Errorf("error reading response: %w", err)
		}
	}

	bodyStr := hs.respBody.String()
	hs.respBody.Reset()

	strs := strings.Split(bodyStr, "\t")

	return Response{ReturnCode: strs[0], Data: strs[1:]}, nil
}
