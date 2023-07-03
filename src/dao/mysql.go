package dao

import (
	"fmt"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
	"sync"
	"time"
)

var (
	linkOnce sync.Once
)

const (
	LocationCheatType = iota
	MultiAccountType
)
const (
	PunishStatus = iota
	ExemptStatus
)

type Govern struct {
	gorm.Model
	PunishType int32  `gorm:"punish_type"`
	Status     int32  `gorm:"status"`
	StartTime  int64  `gorm:"start_time"`
	EndTime    int64  `gorm:"end_time"`
	UserId     string `gorm:"user_id"`
}

func (t Govern) TableName() string {
	return "govern"
}

type GovernDb struct {
	db *gorm.DB
}

func NewGovernDb() *GovernDb {
	return &GovernDb{}
}

func (g *GovernDb) CreateGovernTable() {
	if err := g.db.AutoMigrate(&Govern{}); err != nil {
		fmt.Printf("create Govern failed\n")
		panic(err)
	}
}

func (g *GovernDb) LinkDb() {
	linkOnce.Do(func() {
		dsn := "root:123456@(127.0.0.1:3306)/tiktok?charset=utf8&parseTime=True&loc=Local"
		var err error
		g.db, err = gorm.Open(mysql.Open(dsn), &gorm.Config{})
		if err != nil {
			fmt.Printf("connect mysql failed\n")
			panic(err)
		}
	})
	fmt.Printf("Db link success\n")
}

func (g *GovernDb) UserPunish(userId string, status int32, punishType int32, lastTime int64) {
	g.db.Transaction(func(tx *gorm.DB) error {
		var old Govern
		if err := g.db.Where("user_id = ?", userId).First(&old).Error; err == gorm.ErrRecordNotFound {
			fmt.Printf("Not found record in db, userId = " + userId + ", so create new record\n")
			if err = g.AddGovernRecord(userId, status, punishType, lastTime); err != nil {
				return err
			}
		} else if old.EndTime <= time.Now().Unix()+lastTime {
			fmt.Printf("New punish last longer, so update record \n")
			if err = g.UpdateGovernEndTime(userId, time.Now().Unix()+lastTime); err != nil {
				return err
			}
			if err = g.UpdateGovernStatus(userId, PunishStatus); err != nil {
				return err
			}
		} else {
			fmt.Printf("Ignore\n")
		}
		return nil
	})

}

func (g *GovernDb) AddGovernRecord(userId string, status int32, punishType int32, lastTime int64) error {
	current := time.Now().Unix()
	record := Govern{
		PunishType: punishType,
		Status:     status,
		StartTime:  current,
		EndTime:    current + lastTime,
		UserId:     userId,
	}
	var err error
	if err = g.db.Model(&Govern{}).Create(&record).Error; err != nil {
		fmt.Printf("add Govern record failed\n")
	}
	return err
}

func (g *GovernDb) UpdateGovernEndTime(userId string, endTime int64) error {
	err := g.db.Model(&Govern{}).Where("user_id = ?", userId).Update("end_time", endTime).Error
	return err
}

func (g *GovernDb) UpdateGovernStatus(userId string, status int32) error {
	var err error
	err = g.db.Model(&Govern{}).Where("user_id = ?", userId).Update("status", status).Error
	return err
}
