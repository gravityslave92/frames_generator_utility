package main

import (
	"frames_generator/cmd/frames_generator"
	"runtime"
)

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	frames_generator.GenerateFrames()
}
