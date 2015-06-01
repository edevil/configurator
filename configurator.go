package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	"github.com/samuel/go-zookeeper/zk"
)

const nodeMode = 0744

func doDelete(c *zk.Conn, serverPrefix *string) {
	children, stat, err := c.Children(*serverPrefix)
	if err != nil {
		if err == zk.ErrNoNode {
			fmt.Printf("Path %s not there\n", *serverPrefix)
			return
		}
		panic(err)
	}

	for _, child := range children {
		fullpath := path.Join(*serverPrefix, child)
		doDelete(c, &fullpath)
	}

	fmt.Printf("Will delete %s\n", *serverPrefix)
	c.Delete(*serverPrefix, stat.Version)
}

func ensureRemotePath(c *zk.Conn, serverPrefix *string) {
	if *serverPrefix == "/" {
		return
	}

	dir := path.Dir(*serverPrefix)
	ensureRemotePath(c, &dir)
	if _, err := c.Create(*serverPrefix, nil, 0, zk.AuthACL(zk.PermAll)); err != nil {
		if err == zk.ErrNodeExists {
			fmt.Printf("Dir already created: %s\n", *serverPrefix)
		} else {
			panic(err)
		}
	}
}

func doUpload(c *zk.Conn, serverPrefix *string, localPrefix *string) {
	// iterate local dir
	absLocal, err := filepath.Abs(*localPrefix)
	if err != nil {
		panic(err)
	}

	ensureRemotePath(c, serverPrefix)

	visitFunc := func(visitedPath string, fInfo os.FileInfo, err error) error {
		if !fInfo.Mode().IsRegular() && !fInfo.IsDir() {
			fmt.Printf("Node is not a regular file: %s\n", visitedPath)
			return err
		}

		remotePath := path.Join(*serverPrefix, visitedPath[len(absLocal):])

		// upload files
		var fData []byte
		if fInfo.IsDir() {
			fData = []byte{}
		} else {
			data, err := ioutil.ReadFile(visitedPath)
			if err != nil {
				panic(err)
			} else {
				fData = data
			}
		}

		if _, err := c.Create(remotePath, fData, 0, zk.AuthACL(zk.PermAll)); err != nil {
			if err == zk.ErrNodeExists {
				if fInfo.IsDir() {
					fmt.Printf("Dir already there: %s\n", remotePath)
				} else {
					_, fStat, err := c.Exists(remotePath)
					if err != nil {
						panic(err)
					} else if fStat.NumChildren > 0 {
						panic("Remote path is a dir when a file is expected: " + remotePath)
					}

					if _, err := c.Set(remotePath, fData, fStat.Version); err != nil {
						panic(err)
					}
					fmt.Printf("Overwrote %s -> %s\n", visitedPath, remotePath)
				}
			} else {
				panic(err)
			}
		} else {
			fmt.Printf("Copied %s -> %s\n", visitedPath, remotePath)
		}

		return err
	}
	if err := filepath.Walk(absLocal, visitFunc); err != nil {
		panic(err)
	}
}

func doDownload(c *zk.Conn, serverPrefix *string, localPrefix *string) {
	// iterate remote dir
	fData, stat, err := c.Get(*serverPrefix)
	if err != nil {
		if err == zk.ErrNoNode {
			fmt.Printf("Path %s not there\n", *serverPrefix)
			return
		}
		panic(err)
	}

	if stat.DataLength == 0 {
		// create dir
		if err := os.Mkdir(*localPrefix, nodeMode); err != nil {
			if os.IsExist(err) {
				fmt.Printf("Local dir already present: %s\n", *localPrefix)
			} else {
				panic(err)
			}
		} else {
			fmt.Printf("Created local dir: %s\n", *localPrefix)
		}

		// iterate children
		if stat.NumChildren > 0 {
			children, _, err := c.Children(*serverPrefix)
			if err != nil {
				panic(err)
			}

			for _, child := range children {
				fullpath := path.Join(*serverPrefix, child)
				fulllocalpath := path.Join(*localPrefix, child)
				doDownload(c, &fullpath, &fulllocalpath)
			}

		}
	} else {
		if err := ioutil.WriteFile(*localPrefix, fData, 0644); err != nil {
			panic(err)
		}
		fmt.Printf("Downloaded file: %s", *localPrefix)
	}
}

func main() {
	serversPtr := flag.String("servers", "localhost", "Zookeeper server list")
	authPtr := flag.String("auth", "", "Auth infomation sent to server")
	serverPrefix := flag.String("server_prefix", "/discodev", "Server prefix for config")
	localPrefix := flag.String("local_prefix", "/", "Local prefix for config")
	isUpload := flag.Bool("upload", false, "Upload config to server?")
	isDelete := flag.Bool("delete", false, "Clean remote before upload?")

	flag.Parse()

	c, _, err := zk.Connect(strings.Split(*serversPtr, ","), 5*time.Second)
	if err != nil {
		panic(err)
	}

	if *authPtr != "" {
		err = c.AddAuth("digest", []byte(*authPtr))
		if err != nil {
			panic(err)
		}
	}

	if *isUpload {
		if *isDelete {
			doDelete(c, serverPrefix)
		}
		doUpload(c, serverPrefix, localPrefix)
	} else {
		doDownload(c, serverPrefix, localPrefix)
	}

	c.Close()
	fmt.Println("Connection closed")
}
