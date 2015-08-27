package main

import (
	"fmt"
	"github.com/influxdb/influxdb/client"
	"log"
	"net/url"
	"time"
	"flag"
)

func DBclient(host, port string) *client.Client {

	u, err := url.Parse(fmt.Sprintf("http://%s:%s", host, port))
	if err != nil {
		log.Fatal(err)
	}

	info := client.Config{
		URL:      *u,
		Username: "hewei",
		Password: "19900405",
	}

	con, err1 := client.NewClient(info)
	if err1 != nil {
		log.Fatal(err)
	}

	return con
}

func Getmeasurements(c *client.Client, db1, cmd string) []string {
	q := client.Query{
		Command:  cmd,
		Database: db1,
	}
	var measurements []string

	//show measurements, get them
	response, err := c.Query(q)
	if err == nil {
		if response.Error() != nil {
			fmt.Println(response.Error())
		}
		res := response.Results

		a := res[0].Series[0].Values
		for _, row := range a {
			b := row[0].(string)
			measurements = append(measurements, b)
		}
	}
	return measurements
}

func ReadDB(c *client.Client, db1, db2, cmd string) client.BatchPoints {

	q := client.Query{
		Command:  cmd,
		Database: db1,
	}
	var outer client.BatchPoints
	response, err := c.Query(q)
	if err == nil {
		if response.Error() != nil {
			fmt.Println(response.Error())
		}
		res := response.Results

		for _, k := range res[0].Series {

			var inner client.Point
			inner.Measurement = k.Name
			inner.Tags = k.Tags
			for _, j := range k.Values {
				inner.Time, _ = time.Parse(time.RFC3339, j[0].(string))

				field := make(map[string]interface{})
				l := len(j)
				for i := 1; i < l; i++ {
					if j[i] != nil {
						field[k.Columns[i]] = j[i]
					}
				}
				inner.Fields = field
				inner.Precision = "s"
				outer.Points = append(outer.Points, inner)
			}
		}
		outer.Database = db2
		outer.RetentionPolicy = "default"
	}
	return outer
}

func WriteDB(c *client.Client, b client.BatchPoints) {

	_, err := c.Write(b)
	if err != nil {
		log.Fatal(err)
	}
}

func main() {
	//host := "localhost"
	//port := "8086"
	//db1 := "mydb"
	//db2 := "yourdb"

	//support to input src and dest DB
	src := flag.String("s","127.0.0.1", "input an ip of source DB, from which you want to output datas")
	dest := flag.String("d", "127.0.0.1","input an ip of destination DB, from which you want to input datas")
	sport := flag.String("sport", "8086", "input a port of source DB,from which you want to output datas")
	dport := flag.String("dport", "8086", "input a port of destination DB,from which you want to input datas")
	sdb := flag.String("sdb","mydb", "input name of source DB, from which you want to output datas")
	ddb := flag.String("ddb", "yourdb","input name of destination DB, from which you want to input datas")
      
	flag.Parse()

	scon := DBclient(*src, *sport)
	dcon := DBclient(*dest, *dport)

	getmeasurements := "show measurements"
	x := Getmeasurements(scon, *sdb, getmeasurements)
	for _, m := range x {
		getvalues := fmt.Sprintf("select * from  %s", m)
		y := ReadDB(scon, *sdb, *ddb, getvalues)
		WriteDB(dcon, y)
	}
}
