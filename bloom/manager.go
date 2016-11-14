package bloom

/*
 *  @Date    : 2016-11-13 19:36:30
 *  @Author  : Zhao Yulong (elysium.zyl@gmail.com)
 *  @Link    : ${link}
 *  @Describe: rotated bloomfilter manager
 */

import (
	"error"
	"fmt"
	"os"
)

type BloomFilter interface {
	Test(data []byte) bool
	Add(data []byte) *BloomFilter
	TestAndAdd()
	//info interface
	Capacity() uint
	K() uint
	Count() uint
	EstimatedFillRatio() float64
	FillRatio() float64
}
