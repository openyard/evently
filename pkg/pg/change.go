package pg

import (
	"database/sql"
	"log"
	"time"
)

type Change struct {
}

type ChangeSet map[string][]string

func NewChange() *Change {
	return &Change{}
}

func (c *Change) Install(db *sql.DB, changeSet ChangeSet) error {
	conn := db
	if err := c.assertChangeRepository(conn); err != nil {
		return err
	}
	for id, change := range changeSet {
		old, err := c.applyChange(db, id, change)
		if err != nil {
			log.Printf("!! could not apply change %q - %s", id, err.Error())
			log.Printf("!!!! migration abort !!!!")
			return err
		}
		if old {
			log.Printf("-- change %q already loaded", id)
		} else {
			log.Printf("** change %q applied", id)
		}
	}
	log.Printf("---- migration complete ----")
	return nil

}

func (c *Change) assertChangeRepository(db *sql.DB) error {
	var migrationsCount string
	err := db.QueryRow(`select count(*) from CHANGES`).Scan(&migrationsCount)
	if err == nil {
		log.Printf("---- starting migration on a database of %s migrations", migrationsCount)
		return nil
	}
	query := `create table CHANGES (
		  ID VARCHAR(128) not null unique, 
		  STAMP VARCHAR(35) not null)`
	_, err = db.Exec(query)
	if err != nil {
		log.Printf("could not create change repository - %s", err.Error())
		return err
	}
	return nil
}

func (c *Change) applyChange(db *sql.DB, id string, commands []string) (bool, error) {
	exists, err := c.changeExists(db, id)
	if err != nil {
		return false, err
	}
	if exists {
		return true, nil
	}
	tx, err := db.Begin()
	if err != nil {
		return false, err
	}
	for _, command := range commands {
		_, err = tx.Exec(command)
		if err != nil {
			tx.Rollback()
			return false, err
		}
	}
	_, err = tx.Exec(`insert into CHANGES (ID, STAMP) values ($1, $2)`, id, time.Now().Format(time.RFC3339Nano))
	if err != nil {
		tx.Rollback()
		return false, err
	}
	tx.Commit()
	return false, err
}

func (c *Change) changeExists(db *sql.DB, id string) (bool, error) {
	rows, err := db.Query(`select 1 from CHANGES where ID=$1`, id)
	if err != nil {
		return false, err
	}
	defer rows.Close()
	return rows.Next(), nil
}
