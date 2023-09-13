package reader

import (
	"github.com/parquet-go/parquet-go"
	"github.com/vesoft-inc/nebula-importer/v4/pkg/source"
	"github.com/vesoft-inc/nebula-importer/v4/pkg/spec"
)

type (
	parquetReader struct {
		*baseReader
		pr *parquet.Reader
	}

	parquetSource struct {
		s source.Source
	}
)

func (s *parquetSource) ReadAt(p []byte, off int64) (n int, err error) {
	return s.s.Read(p)
}

func NewParquetReader(s source.Source) RecordReader {
	ps := &parquetSource{s: s}
	pr := parquet.NewReader(ps)

	return &parquetReader{
		baseReader: &baseReader{
			s: s,
		},
		pr: pr,
	}
}

func (r *parquetReader) Size() (int64, error) {
	return r.s.Size()
}

func (r *parquetReader) Read() (int, spec.Record, error) {
	var record spec.Record
	err := r.pr.Read()
	return r.rr.Take(r.br.Buffered()), record, err
}
