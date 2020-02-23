package frames_generator

import (
	"frames_generator/config"
	"frames_generator/internal/database"
	ri "frames_generator/internal/road_infrastructure"
	"github.com/spf13/viper"
	"log"
	"time"
)

// Enter point to spin the utility for frames generation.
func GenerateFrames() {
	config.SetupConfig()

	if errGenHardFrames := database.GenerateHardFrames(); errGenHardFrames != nil {
		log.Fatalln(errGenHardFrames)
	}

	semaphoreLimit := viper.GetInt32("pg.max_connections_limit")
	signs, errQuerySigns := ri.SelectSignsWithoutFrames()
	if errQuerySigns != nil {
		log.Fatalln(errQuerySigns)
	}

	semaphore := make(chan int, semaphoreLimit)
	for index, sign := range signs {
		semaphore <- index
		go sign.LinkToFrame(semaphore)
	}

	ticker := time.NewTicker(2000 * time.Millisecond)

	for {
		select {
		case <-ticker.C:
			if len(semaphore) == 0 {
				break
			}
		}
	}

}
