package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"time"
	_ "github.com/go-sql-driver/mysql"
	"github.com/xtls/xray-core/app/proxyman/command"
	statsService "github.com/xtls/xray-core/app/stats/command"
	"github.com/xtls/xray-core/common/protocol"
	"github.com/xtls/xray-core/common/serial"
	"github.com/xtls/xray-core/proxy/shadowsocks"
	"google.golang.org/grpc"
)

type BaseConfig struct {
	APIAddress string
	APIPort    uint16
}

type UserInfo struct {
	Level    uint32
	InTag    string
	Email    string
	Password string
}

type TrafficData struct {
	Download int64
	Upload   int64
}

type XrayController struct {
	HsClient    command.HandlerServiceClient
	StatsClient statsService.StatsServiceClient
	CmdConn     *grpc.ClientConn
}

func (xrayCtl *XrayController) Init(cfg *BaseConfig) (err error) {
	xrayCtl.CmdConn, err = grpc.Dial(fmt.Sprintf("%s:%d", cfg.APIAddress, cfg.APIPort), grpc.WithInsecure())
	if err != nil {
		return err
	}

	xrayCtl.HsClient = command.NewHandlerServiceClient(xrayCtl.CmdConn)
	xrayCtl.StatsClient = statsService.NewStatsServiceClient(xrayCtl.CmdConn)

	return
}

func addSSUser(client command.HandlerServiceClient, user *UserInfo) error {
	_, err := client.AlterInbound(context.Background(), &command.AlterInboundRequest{
		Tag: user.InTag,
		Operation: serial.ToTypedMessage(&command.AddUserOperation{
			User: &protocol.User{
				Level: user.Level,
				Email: user.Email,
				Account: serial.ToTypedMessage(&shadowsocks.Account{
					Password:   user.Password,
					CipherType: shadowsocks.CipherType_AES_128_GCM,
				}),
			},
		}),
	})
	return err
}

func removeSSUser(client command.HandlerServiceClient, email string, inTag string) error {
	_, err := client.AlterInbound(context.Background(), &command.AlterInboundRequest{
		Tag: inTag,
		Operation: serial.ToTypedMessage(&command.RemoveUserOperation{
			Email: email,
		}),
	})
	return err
}

func queryTraffic(c statsService.StatsServiceClient, ptn string, reset bool) (int64, error) {
	traffic := int64(-1)
	resp, err := c.QueryStats(context.Background(), &statsService.QueryStatsRequest{
		Pattern: ptn,
		Reset_:  reset,
	})
	if err != nil {
		return traffic, err
	}
	stat := resp.GetStat()
	if len(stat) != 0 {
		traffic = stat[0].Value
	}
	return traffic, nil
}

func getUsersFromDB(db *sql.DB) ([]UserInfo, error) {
	rows, err := db.Query("SELECT port, passwd FROM user WHERE enable = 1")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var users []UserInfo
	for rows.Next() {
		var user UserInfo
		var port int
		var passwd string
		if err := rows.Scan(&port, &passwd); err != nil {
			return nil, err
		}
		user.Email = fmt.Sprintf("%d", port)
		user.Password = fmt.Sprintf("%d%s", port, passwd)
		user.InTag = "ssapi"
		user.Level = 0
		users = append(users, user)
	}

	return users, nil
}

func getCurrentSSUsers() ([]UserInfo, error) {
	file, err := ioutil.ReadFile("current_users.json")
	if err != nil {
		if os.IsNotExist(err) {
			return []UserInfo{}, nil
		}
		return nil, err
	}

	var users []UserInfo
	err = json.Unmarshal(file, &users)
	if err != nil {
		return nil, err
	}

	return users, nil
}

func saveCurrentSSUsers(users []UserInfo) error {
	file, err := json.MarshalIndent(users, "", "  ")
	if err != nil {
		return err
	}

	err = ioutil.WriteFile("current_users.json", file, 0644)
	if err != nil {
		return err
	}

	return nil
}

func updateTraffic(db *sql.DB, user UserInfo, traffic TrafficData) error {
	if traffic.Download > 100 || traffic.Upload > 100 {
		_, err := db.Exec("UPDATE user SET d = d + ?, u = u + ? WHERE port = ?", traffic.Download, traffic.Upload, user.Email)
		return err
	}
	return nil
}

func synchronizeUsers(client command.HandlerServiceClient, statsClient statsService.StatsServiceClient, db *sql.DB, dbUsers, currentUsers []UserInfo) error {
	dbUserMap := make(map[string]UserInfo)
	for _, user := range dbUsers {
		dbUserMap[user.Email] = user
	}

	currentUserMap := make(map[string]UserInfo)
	for _, user := range currentUsers {
		currentUserMap[user.Email] = user
	}

	// Add or update users from the database
	for email, user := range dbUserMap {
		currentUser, exists := currentUserMap[email]
		if !exists {
			fmt.Printf("[%s] Adding user: %s\n", time.Now().Format(time.RFC3339), email)
			if err := addSSUser(client, &user); err != nil {
				fmt.Printf("[%s] Failed to add user: %s, error: %v\n", time.Now().Format(time.RFC3339), email, err)
			} else {
				currentUserMap[email] = user
			}
		} else if currentUser.Password != user.Password {
			fmt.Printf("[%s] Updating user password: %s\n", time.Now().Format(time.RFC3339), email)
			if err := removeSSUser(client, email, "ssapi"); err != nil {
				fmt.Printf("[%s] Failed to remove user for update: %s, error: %v\n", time.Now().Format(time.RFC3339), email, err)
			} else {
				if err := addSSUser(client, &user); err != nil {
					fmt.Printf("[%s] Failed to add user after update: %s, error: %v\n", time.Now().Format(time.RFC3339), email, err)
				} else {
					currentUserMap[email] = user
				}
			}
		}

		// 查询并上报流量
		uplinkPattern := fmt.Sprintf("user>>>%s>>>traffic>>>uplink", email)
		downlinkPattern := fmt.Sprintf("user>>>%s>>>traffic>>>downlink", email)
		uplink, err := queryTraffic(statsClient, uplinkPattern, true)
		if err != nil {
			fmt.Printf("[%s] Failed to query uplink traffic for user: %s, error: %v\n", time.Now().Format(time.RFC3339), email, err)
		}
		downlink, err := queryTraffic(statsClient, downlinkPattern, true)
		if err != nil {
			fmt.Printf("[%s] Failed to query downlink traffic for user: %s, error: %v\n", time.Now().Format(time.RFC3339), email, err)
		}

		traffic := TrafficData{
			Download: downlink,
			Upload:   uplink,
		}
		if err := updateTraffic(db, user, traffic); err != nil {
			fmt.Printf("[%s] Failed to update traffic for user: %s, error: %v\n", time.Now().Format(time.RFC3339), email, err)
		}
	}

	// Remove users not in the database
	for email := range currentUserMap {
		if _, exists := dbUserMap[email]; !exists {
			fmt.Printf("[%s] Removing user: %s\n", time.Now().Format(time.RFC3339), email)
			if err := removeSSUser(client, email, "ssapi"); err != nil {
				fmt.Printf("[%s] Failed to remove user: %s, error: %v\n", time.Now().Format(time.RFC3339), email, err)
			} else {
				delete(currentUserMap, email)
			}
		}
	}

	var updatedUsers []UserInfo
	for _, user := range currentUserMap {
		updatedUsers = append(updatedUsers, user)
	}

	return saveCurrentSSUsers(updatedUsers)
}

func main() {
	// 删除current_users.json文件
	if err := os.Remove("current_users.json"); err != nil && !os.IsNotExist(err) {
		fmt.Printf("[%s] 删除 current_users.json 失败: %v\n", time.Now().Format(time.RFC3339), err)
		return
	}

	// 配置数据库连接
	dsn := "DBUSER:DBPASSWD@tcp(dbserver.com)/DBNAME"
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		fmt.Printf("[%s] 数据库连接失败: %v\n", time.Now().Format(time.RFC3339), err)
		return
	}
	defer db.Close()

	// 配置Xray API连接
	cfg := &BaseConfig{
		APIAddress: "127.0.0.1",
		APIPort:    9085,
	}

	xrayCtl := &XrayController{}
	err = xrayCtl.Init(cfg)
	if err != nil {
		fmt.Printf("[%s] 初始化 XrayController 失败: %v\n", time.Now().Format(time.RFC3339), err)
		return
	}

	// 创建一个每30秒触发一次的ticker
	ticker := time.NewTicker(60 * time.Second)
	defer ticker.Stop()

	for {
		// 获取数据库中的用户
		dbUsers, err := getUsersFromDB(db)
		if err != nil {
			fmt.Printf("[%s] 获取数据库用户失败: %v\n", time.Now().Format(time.RFC3339), err)
			continue
		}

		// 获取当前Xray中的用户
		currentUsers, err := getCurrentSSUsers()
		if err != nil {
			fmt.Printf("[%s] 获取当前Xray用户失败: %v\n", time.Now().Format(time.RFC3339), err)
			continue
		}

		// 同步用户并上报流量
		err = synchronizeUsers(xrayCtl.HsClient, xrayCtl.StatsClient, db, dbUsers, currentUsers)
		if err != nil {
			fmt.Printf("[%s] 同步用户失败: %v\n", time.Now().Format(time.RFC3339), err)
		} else {
			fmt.Printf("[%s] 用户同步成功\n", time.Now().Format(time.RFC3339))
		}

		<-ticker.C
	}
}
