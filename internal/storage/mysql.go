package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"event-pipeline/internal/pipeline"
	"event-pipeline/pkg/logger"
	"fmt"

	_ "github.com/go-sql-driver/mysql"
)

type MySQLStorage struct {
	db *sql.DB
}

func NewMySQLStorage(dsn string) (*MySQLStorage, error) {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open mysql connection: %w", err)
	}
	// tune pool
	db.SetMaxOpenConns(20)
	db.SetMaxIdleConns(10)

	logger.Get().Infow("mysql storage initialized", "dsn", dsn)
	return &MySQLStorage{db: db}, nil
}

func (s *MySQLStorage) DB() *sql.DB {
	return s.db
}

func (s *MySQLStorage) Close() error {
	return s.db.Close()
}

func (s *MySQLStorage) Store(ctx context.Context, events []pipeline.ProcessedEvent) error {
	log := logger.Get().With("component", "mysql_storage")

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		log.Errorw("begin transaction failed", "error", err)
		return err
	}

	stmt, err := tx.PrepareContext(ctx, `
		INSERT INTO processed_events
		(id, type, source, user_id, processed_data, processing_time_ms, created_at, processed_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)
	`)
	if err != nil {
		_ = tx.Rollback()
		log.Errorw("prepare statement failed", "error", err)
		return err
	}
	defer stmt.Close()

	for _, e := range events {
		dataBytes, err := json.Marshal(e.Data)
		if err != nil {
			_ = tx.Rollback()
			log.Errorw("failed to marshal event data",
				"event_id", e.ID,
				"error", err,
			)
			return fmt.Errorf("failed to marshal data: %w", err)
		}

		_, err = stmt.ExecContext(ctx,
			e.ID,
			e.Type,
			e.Source,
			e.UserID,
			string(dataBytes),
			e.ProcessingTimeMS,
			e.Timestamp,
			e.ProcessedAt,
		)
		if err != nil {
			_ = tx.Rollback()
			log.Errorw("insert failed",
				"event_id", e.ID,
				"type", e.Type,
				"source", e.Source,
				"error", err,
			)
			return fmt.Errorf("insert failed: %w", err)
		}

		log.Debugw("event stored",
			"event_id", e.ID,
			"type", e.Type,
			"source", e.Source,
			"processing_time_ms", e.ProcessingTimeMS,
		)
	}

	if err := tx.Commit(); err != nil {
		log.Errorw("transaction commit failed", "error", err)
		return err
	}

	log.Infow("batch stored successfully", "count", len(events))
	return nil
}
