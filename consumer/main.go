package main

import (
	"consumer/repositories"
	"consumer/services"
	"context"
	"events"
	"fmt"
	"strings"

	"github.com/Shopify/sarama"
	"github.com/spf13/viper"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func init() {
	viper.SetConfigName("config")
	viper.SetConfigType("yaml")
	viper.AddConfigPath(".")
	viper.AutomaticEnv()
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))

	err := viper.ReadInConfig()
	if err != nil {
		panic(err)
	}
}

func initDatabase() *gorm.DB {
	dsn := fmt.Sprintf("%v://%v:%v@%v/%v?sslmode=disable",
		viper.GetString("db.driver"),
		viper.GetString("db.username"),
		viper.GetString("db.password"),
		viper.GetString("db.host"),
		viper.GetString("db.database"),
	)

	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	if err != nil {
		panic(err)
	}
	return db
}

func main() {
	consumer, err := sarama.NewConsumerGroup(viper.GetStringSlice("kafka.servers"), viper.GetString("kafka.group"), nil)
	if err != nil {
		panic(err)
	}
	defer consumer.Close()

	db := initDatabase()
	accountRepo := repositories.NewAccountRepository(db)
	accountHandler := services.NewAccountEventHandler(accountRepo)
	accountConsumerHandler := services.NewConsumerHandler(accountHandler)

	fmt.Println("startooooo")

	for {
		consumer.Consume(context.Background(), events.Topics, accountConsumerHandler)
	}
	// repositories.NewAccountRepository()
	// services.NewAccountEventHandler()
	// services.NewConsumerHandler()
}

// func main() {

// 	servers := []string{"localhost:9092"}

// 	consumer, err := sarama.NewConsumer(servers, nil)

// 	if err != nil {
// 		panic(err)
// 	}

// 	partition, err := consumer.ConsumePartition("aorhi", 0, sarama.OffsetNewest)

// 	if err != nil {
// 		fmt.Println("err")
// 		panic(err)
// 	}

// 	defer partition.Close()
// 	for {
// 		select {
// 		case err := <-partition.Errors():
// 			fmt.Println("err")
// 			fmt.Println(err)
// 		case msg := <-partition.Messages():
// 			fmt.Println(string(msg.Value))
// 		}
// 	}
// }
