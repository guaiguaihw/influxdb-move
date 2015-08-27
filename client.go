package main

import (
	"flag"
	"fmt"
	"github.com/influxdb/influxdb/client"
	"log"
	"net/url"
	"time"
)

func DBclient(host, port string) *client.Client {

	//connect to database
	u, err := url.Parse(fmt.Sprintf("http://%s:%s", host, port))
	if err != nil {
		log.Fatal(err)
	}

	info := client.Config{
		URL: *u,
	}

	var con *client.Client
	con, err = client.NewClient(info)
	if err != nil {
		log.Fatal(err)
	}

	return con
}

func Getmeasurements(c *client.Client, sdb, cmd string) []string {

	//get measurements from database
	q := client.Query{
		Command:  cmd,
		Database: sdb,
	}
	var measurements []string

	response, err := c.Query(q)
	if err == nil {
		res := response.Results

		values := res[0].Series[0].Values
		for _, row := range values {
			measurement := row[0].(string)
			measurements = append(measurements, measurement)
		}
	} else {
		log.Fatal(err)
	}
	return measurements
}

func ReadDB(c *client.Client, sdb, ddb, cmd string) client.BatchPoints {

	q := client.Query{
		Command:  cmd,
		Database: sdb,
	}

	//get type client.BatchPoints
	var batchpoints client.BatchPoints

	response, err := c.Query(q)
	if err == nil {

		res := response.Results

		for _, ser := range res[0].Series {

			//get type client.Point
			var point client.Point

			point.Measurement = ser.Name
			point.Tags = ser.Tags
			for _, v := range ser.Values {
				point.Time, _ = time.Parse(time.RFC3339, v[0].(string))

				field := make(map[string]interface{})
				l := len(v)
				for i := 1; i < l; i++ {
					if v[i] != nil {
						field[ser.Columns[i]] = v[i]
					}
				}
				point.Fields = field
				point.Precision = "s"
				batchpoints.Points = append(batchpoints.Points, point)
			}
		}
		batchpoints.Database = ddb
		batchpoints.RetentionPolicy = "default"
	} else {

		log.Fatal(err)
	}
	return batchpoints
}

func WriteDB(c *client.Client, b client.BatchPoints) {

	_, err := c.Write(b)
	if err != nil {
		log.Fatal(err)
	}
}

func main() {

	//support to input src and dest DB
	src := flag.String("s", "127.0.0.1", "input an ip of source DB, from which you want to output datas")
	dest := flag.String("d", "127.0.0.1", "input an ip of destination DB, from which you want to input datas")
	sport := flag.String("sport", "8086", "input a port of source DB,from which you want to output datas")
	dport := flag.String("dport", "8086", "input a port of destination DB,from which you want to input datas")
	sdb := flag.String("sdb", "mydb", "input name of source DB, from which you want to output datas")
	ddb := flag.String("ddb", "yourdb", "input name of destination DB, from which you want to input datas")
	st := flag.String("sT", "'1970-01-01'", "input a start time ,from when you want to select datas")
	et := flag.String("eT", "'2100-01-01'", "input an end time, until when you want to select datas")

	flag.Parse()

	scon := DBclient(*src, *sport)
	dcon := DBclient(*dest, *dport)

	getmeasurements := "show measurements"
	measurements := Getmeasurements(scon, *sdb, getmeasurements)
	for _, m := range measurements {
		getvalues := fmt.Sprintf("select * from  %s where time > '%s' and time < '%s'", m, *st, *et)
		batchpoints := ReadDB(scon, *sdb, *ddb, getvalues)
		WriteDB(dcon, batchpoints)
	}
}
