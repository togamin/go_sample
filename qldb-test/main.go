package main

import (
	"context"
	"fmt"

	"github.com/amzn/ion-go/ion"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/qldbsession"
	"github.com/awslabs/amazon-qldb-driver-go/qldbdriver"
)

func main() {
	fmt.Println("QLDBへのアクセステスト")

	//ドライバーのインスタンス化
	awsSession := session.Must(session.NewSession(aws.NewConfig().WithRegion("ap-northeast-1")))
	qldbSession := qldbsession.New(awsSession)

	driver, err := qldbdriver.New(
		"qldb-test",
		qldbSession,
		func(options *qldbdriver.DriverOptions) {
			options.LoggerVerbosity = qldbdriver.LogInfo
		})
	if err != nil {
		panic(err)
	}

	//QLDBからのデータの読み出し処理
	var decodedResult map[string]interface{}
	_, err = driver.Execute(context.Background(), func(txn qldbdriver.Transaction) (interface{}, error) {
		result, err := txn.Execute("SELECT * FROM QLDB_TEST_TABLE WHERE ID = ?", "1")

		if err != nil {
			panic(err)
		}
		for result.Next(txn) {
			ionBinary := result.GetCurrentData()
			err = ion.Unmarshal(ionBinary, &decodedResult)
			if err != nil {
				return nil, err
			}
		}
		if result.Err() != nil {
			return nil, result.Err()
		}
		return nil, nil
	})

	fmt.Println(decodedResult)
}
