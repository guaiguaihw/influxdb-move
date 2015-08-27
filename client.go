package main

import (
	"fmt"
	"github.com/influxdb/influxdb/client"
	"log"
	"net/url"
	"time"
	//"flag"
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
	host := "localhost"
	port := "8086"
	con := DBclient(host, port)
	db1 := "mydb"
	db2 := "yourdb"

	getmeasurements := "show measurements"
	x := Getmeasurements(con, db1, getmeasurements)
	for _, m := range x {
		getvalues := fmt.Sprintf("select * from  %s", m)
		y := ReadDB(con, db1, db2, getvalues)
		WriteDB(con, y)
	}
}
