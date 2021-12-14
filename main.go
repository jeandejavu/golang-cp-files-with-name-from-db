package main

import (
	"database/sql"
	"fmt"
	"io"
	"log"
	"os"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/joho/godotenv"
)

type TitularContrato struct {
	Codigo string
	Imagem string
}

func dsn() string {
	username := os.Getenv("DB_USER")
	password := os.Getenv("DB_PASSWORD")
	hostname := os.Getenv("DB_HOST")
	dbName := os.Getenv("DB_NAME")

	return fmt.Sprintf("%s:%s@tcp(%s)/%s", username, password, hostname, dbName)
}

func dbConn() (db *sql.DB) {
	db, err := sql.Open("mysql", dsn())
	if err != nil {
		panic(err.Error())
	}
	return db
}

func pegarDados() (titularContrato []TitularContrato) {
	db := dbConn()
	defer db.Close()

	rows, err := db.Query(os.Getenv("QUERY"))
	if err != nil {
		panic(err.Error())
	}

	for rows.Next() {
		var codigo string
		var imagem string
		err = rows.Scan(&codigo, &imagem)
		if err != nil {
			panic(err.Error())
		}
		titularContrato = append(titularContrato, TitularContrato{codigo, imagem})
	}
	return titularContrato
}

func carregarEnv() {
	err := godotenv.Load()
	if err != nil {
		log.Fatal("Error loading .env file")
	}
}

func copiarArquivo(nomeArquivo string, nomeDestino string) {
	arquivoOrigem := fmt.Sprintf("%s%s", os.Getenv("PATH_FROM"), nomeArquivo)
	arquivoDestino := fmt.Sprintf("%s%s", os.Getenv("PATH_TO"), nomeDestino)
	err := CopyFile(arquivoOrigem, arquivoDestino)
	if err != nil {
		panic(err.Error())
	}
}

func newError(format string, a ...interface{}) error {
	return fmt.Errorf(format, a...)
}

func CopyFile(arquivoOrigem, arquivoDestino string) error {
	sourceFileStat, err := os.Stat(arquivoOrigem)
	if err != nil {
		return err
	}

	if !sourceFileStat.Mode().IsRegular() {
		return newError("%s is not a regular file", arquivoOrigem)
	}

	source, err := os.Open(arquivoOrigem)
	if err != nil {
		return err
	}
	defer source.Close()

	destination, err := os.Create(arquivoDestino)
	if err != nil {
		return err
	}
	defer destination.Close()
	_, err = io.Copy(destination, source)
	if err != nil {
		return err
	}
	return nil
}

func main() {
	carregarEnv()
	start := time.Now()
	dados := pegarDados()
	total := len(dados)
	fmt.Printf("Tempo execução %s , quantidade de registros %d", time.Since(start), total)

	var wg sync.WaitGroup
	for _, dado := range dados {
		wg.Add(1)
		go func(dado TitularContrato) {
			defer wg.Done()
			copiarArquivo(dado.Imagem, fmt.Sprintf("%s-%s", dado.Codigo, dado.Imagem))
		}(dado)
	}
	wg.Wait()
}
