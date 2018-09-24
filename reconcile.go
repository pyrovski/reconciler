package main

import (
	"bufio"
	"database/sql"
	"flag"
	"log"
	"os"
	"regexp"
	"strings"
	"sync"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

var dbFile string
var dbTimeout time.Duration

// Exclude matched paths from the DB matching this regex
var exclude string

// File format: torrent filename <tab> contained filename

// TODO: match file from query against path/file from DB; the query file may contain directory elements.
const LookupQuery = "select path || '/' || file from files where path || '/' || file like ?"

type torFile struct {
	tor  string
	file string
}

type matchedFile struct {
	tor  string
	path string
}

func matchDBFiles(db *sql.DB, i chan *torFile, o chan *matchedFile, wg *sync.WaitGroup) {
	defer wg.Done()
	stmt, err := db.Prepare(LookupQuery)
	if err != nil {
		log.Print(err)
		return
	}
	// maps torrent files to paths at which torrents should be added
	matches := make(map[string]string)

	exRegex := regexp.MustCompile(exclude)
	for tf := range i {
		if _, ok := matches[tf.tor]; ok {
			// only need one match per torrent
			continue
		}
		log.Printf("querying %q: %q", tf.tor, tf.file)
		rows, err := stmt.Query("%" + tf.file)
		if err != nil {
			log.Fatal(err)
		}
		defer rows.Close()
		for rows.Next() {
			var fullpath string
			if err := rows.Scan(&fullpath); err != nil {
				log.Fatal(err)
			}
			if exclude != "" {
				if exRegex.MatchString(fullpath) {
					log.Printf("Exclude: %q", fullpath)
					continue
				}
			}
			log.Printf("result: %q", fullpath)
			if strings.HasSuffix(fullpath, tf.file) {
				path := strings.TrimSuffix(fullpath, tf.file)
				log.Printf("match: %q", path)
				matches[tf.tor] = path
			}
		}
		if err := rows.Err(); err != nil {
			log.Fatal(err)
		}
	}
}

func scanFiles(db *sql.DB, c chan *torFile, args []string) {
	for _, arg := range args {
		f, _ := os.Open(arg)
		defer f.Close()
		r := bufio.NewScanner(f)
		for r.Scan() {
			line := r.Text()
			ts := strings.Split(line, "\t")
			if len(ts) != 2 {
				log.Printf("invalid line: %q", line)
				continue
			}
			tor := strings.TrimSpace(ts[0])
			tf := strings.TrimSpace(ts[1])
			//log.Printf("%q: %q", tor, tf)
			torf := &torFile{
				tor:  tor,
				file: tf,
			}
			c <- torf
		}
	}

}

func main() {
	flag.StringVar(&dbFile, "db", "", "sqlite3 DB file")
	flag.DurationVar(&dbTimeout, "dbtimeout", time.Duration(30), "timeout for DB operations")
	flag.StringVar(&exclude, "exclude", "", "regex for excluding matched paths from the DB")
	flag.Parse()
	args := flag.Args()
	if len(args) < 1 {
		log.Fatalf("must provide one or more files")
	}
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	if dbFile == "" {
		log.Fatalf("must set --db")
	}
	db, err := sql.Open("sqlite3", dbFile)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	wg := &sync.WaitGroup{}
	wg.Add(1)
	c := make(chan *torFile)
	m := make(chan *matchedFile)
	go matchDBFiles(db, c, m, wg)
	scanFiles(db, c, args)
	wg.Wait()
	close(c)
	close(m)
}
