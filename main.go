package main

import (
	"bufio"
	"errors"
	"log"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"

	"github.com/dustin/go-humanize"
	"gopkg.in/yaml.v3"
)

var S3InfoMap map[string]int64 // [filepash]size

func main() {
	// マスターとkeylistの関連付け
	ret, err := initResult()
	if err != nil {
		log.Fatal(err)
	}
	// データの仮想KVS化
	if err := loadLists(); err != nil {
		log.Fatal(err)
	}
	// keyの抽出・集計用変数へのデータ格納
	csvPaths, err := loadCsvPaths()
	if err != nil {
		log.Fatal(err)
	}
	for _, path := range csvPaths {
		log.Println(path)

		f, err := os.Open(path)
		if err != nil {
			log.Fatal(err)
		}
		defer f.Close()
		scanner := bufio.NewScanner(f)
		for scanner.Scan() {

			list := strings.Split(scanner.Text(), ",")
			if list[0] == "id" {
				continue // header行はskip
			}
			nsID := list[1]
			filePath := list[2]
			_, filename := filepath.Split(path)
			if _, ok := ret.Source.Names[nsID]; !ok {
				continue
			}
			val, ok := ret.Source.Names[nsID].FileSums[filename]
			if !ok {
				val = FileSum{}
			}
			val.Count++
			val.Sum += S3InfoMap[filePath]
			ret.Source.Names[nsID].FileSums[filename] = val
		}
	}

	// 集計・出力
	ret.summarize()
	dir, _ := os.Getwd()
	path := filepath.Join(dir, "result.yaml")
	ret.marshal(path)
	return
}

func loadLists() error {
	dir, err := os.Getwd()
	if err != nil {
		return err
	}
	files, err := os.ReadDir(path.Join(dir, "list"))
	if err != nil {
		return err
	}

	S3InfoMap = make(map[string]int64)
	regStr := "\\s+"
	reg := regexp.MustCompile(regStr)

	for _, f := range files {
		if f.IsDir() {
			continue
		}
		if path.Ext(f.Name()) == ".list" {
			f, err := os.Open(path.Join(dir, "list", f.Name()))
			if err != nil {
				return err
			}
			defer f.Close()
			scanner := bufio.NewScanner(f)
			for scanner.Scan() {
				list := reg.Split(scanner.Text(), -1)
				size, err := strconv.ParseInt(list[2], 10, 64)
				if err != nil {
					return err
				}
				S3InfoMap[list[3]] = size
			}
		}
	}
	return nil
}

func loadCsvPaths() ([]string, error) {
	dir, err := os.Getwd()
	if err != nil {
		return nil, err
	}
	files, err := os.ReadDir(path.Join(dir, "csv"))
	if err != nil {
		return nil, err
	}

	paths := make([]string, 0, len(files))
	for _, f := range files {
		if f.IsDir() {
			continue
		}
		if path.Ext(f.Name()) == ".csv" {
			paths = append(paths, path.Join(dir, "csv", f.Name()))
		}
	}
	return paths, nil
}

type Results struct {
	Source Source `yaml:"source"`
}
type Source struct {
	ID     int             `yaml:"source_id"`
	Names  map[string]Name `yaml:"names"`
	Sum    int64           `yaml:"sum"`
	SumStr string          `yaml:"sum_str"`
}
type Name struct {
	Id       string             `yaml:"id"`
	Name     string             `yaml:"name"`
	FileSums map[string]FileSum `yaml:"file_sumallys"`
	Sum      int64              `yaml:"sum"`
	SumStr   string             `yaml:"sum_str"`
}
type FileSum struct {
	Count int64 `yaml:"count"`
	Sum   int64 `yaml:"sum"`
}

func initResult() (*Results, error) {
	dir, err := os.Getwd()
	if err != nil {
		return nil, err
	}

	files, err := os.ReadDir(path.Join(dir, "target"))
	if err != nil {
		return nil, err
	}
	if len(files) > 1 {
		return nil, errors.New("target directory has more than 1 file")
	}

	sourceList := make([]Source, 0, len(files))
	for _, f := range files {
		if f.IsDir() {
			continue
		}
		id, err := strconv.Atoi(f.Name())
		if err != nil {
			return nil, err
		}
		c := Source{
			ID:    id,
			Names: make(map[string]Name),
		}
		f, err := os.Open(path.Join(dir, "target", f.Name()))
		if err != nil {
			return nil, err
		}
		defer f.Close()
		scanner := bufio.NewScanner(f)
		for scanner.Scan() {
			l := strings.Split(scanner.Text(), ",")
			n := Name{
				Id:       l[0],
				Name:     l[1],
				FileSums: map[string]FileSum{},
			}
			c.Names[l[0]] = n
		}
		sourceList = append(sourceList, c)
	}

	return &Results{Source: sourceList[0]}, nil
}

func (r *Results) summarize() {
	for _, n := range r.Source.Names {
		for _, f := range n.FileSums {
			r.Source.Sum += f.Sum
			val := r.Source.Names[n.Id]
			val.Sum += f.Sum
			r.Source.Names[n.Id] = val
		}
	}
	for _, n := range r.Source.Names {
		val := r.Source.Names[n.Id]
		val.SumStr = humanize.Bytes(uint64(n.Sum))
		r.Source.Names[n.Id] = val
	}
	r.Source.SumStr = humanize.Bytes(uint64(r.Source.Sum))
}

func (r *Results) marshal(path string) error {
	b, err := yaml.Marshal(r)
	if err != nil {
		return err
	}
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	if _, err := f.Write(b); err != nil {
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return nil
}
