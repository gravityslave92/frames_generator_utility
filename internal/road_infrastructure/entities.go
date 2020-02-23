package road_infrastructure

import (
	"context"
	"database/sql"
	"frames_generator/internal/database"
	"github.com/pkg/errors"
	"log"
)

//  Represents necessary for linking sign attributes from ri_signs column
type Sign struct {
	Id        int            `db:"id"`
	FrameKind sql.NullString `db:"frame_kind"`
	Geometry  string         `db:"geometry"`
}

// Describes ri_frames table id column
type Frame struct {
	Id int `db:"id"`
}

// Queries active signs without frames
func SelectSignsWithoutFrames() ([]*Sign, error) {
	conn, errSetupConn := database.SetupDBConnection()
	if errSetupConn != nil {
		log.Fatalln(errSetupConn)
	}

	rows, errSelectSigns := conn.QueryContext(context.Background(), "SELECT id, frame_kind, geometry FROM ri_signs where status=$1 ", "03")
	if errSelectSigns != nil {
		return nil, errors.WithMessage(errSelectSigns, "")
	}
	defer rows.Close()

	var signs []*Sign
	for rows.Next() {
		sign := new(Sign)
		rows.Scan(&sign.Id, &sign.FrameKind, &sign.Geometry)

		signs = append(signs, sign)
	}

	return signs, nil
}

// Export function for linking process between both a sign and a frame. Setups db connection  and a transaction.
// Releases a semaphore upon completion.
func (sign *Sign) LinkToFrame(semaphore <-chan int) {
	defer func() {
		// Release semaphore
		<-semaphore
	}()

	conn, errSetupConn := database.SetupDBConnection()
	if errSetupConn != nil {
		log.Println(errSetupConn)
		return
	}
	defer conn.Close()

	transaction, cancelFn, errSetupTx := database.SetupTransaction(conn)
	if errSetupTx != nil {
		log.Println(errSetupTx)
		return
	}

	if errLinking := sign.linkToFrame(transaction, cancelFn); errLinking != nil {
		log.Println(errLinking)
		return
	}
}

// Real implementation of sing linking process. Utilizes given transaction with cancel function for rollbacking the first upon errors.
func (sign *Sign) linkToFrame(transaction *sql.Tx, cancelFn context.CancelFunc) error {
	frame := Frame{}

	if errQueryFrame := transaction.QueryRow("SELECT * FROM ri_frames WHERE ST_INTERSECTS(ST_BUFFER(geometry::geography, 1), $1) limit 1", sign.Geometry).Scan(&frame); errQueryFrame != nil {
		if errInsertFrame := transaction.QueryRow(`INSERT INTO ri_frames(kind, geometry) VALUES ($1, $2) RETURNING id `, sign.FrameKind, sign.Geometry).Scan(&frame.Id); errInsertFrame != nil {
			cancelFn()
			return errors.WithMessage(errInsertFrame, "Frame creation has failed")
		}
	}

	_, errInsertArrangement := transaction.ExecContext(context.Background(), `insert into ri_sign_frame_arrangements(frame_id, sign_id) VALUES ($1, $2);`, frame.Id, sign.Id)
	if errInsertArrangement != nil {
		cancelFn()
		return errors.WithMessage(errInsertArrangement, "Arrangement creation has failed")
	}

	errTxCommit := transaction.Commit()
	if errTxCommit != nil {
		return errors.WithMessage(errTxCommit, "Transaction of linking  a sign with a frame has failed")
	}

	return nil
}
