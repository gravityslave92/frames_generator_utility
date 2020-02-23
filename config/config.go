package config

import (
	"github.com/spf13/viper"
	"log"
)

func SetupConfig() {
	viper.SetConfigName("config")
	viper.AddConfigPath("./config")
	viper.AutomaticEnv()
	if err := viper.ReadInConfig(); err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); ok {
			log.Fatalln(err)
		}
	}
}
