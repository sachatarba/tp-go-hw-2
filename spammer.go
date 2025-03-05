package main

import (
	"fmt"
	"log"
	"slices"
	"sync"
)

func RunPipeline(cmds ...cmd) {
	out := make(chan interface{})
	close(out)
	wg := &sync.WaitGroup{}

	for _, c := range cmds {
		wg.Add(1)
		in := out
		out = make(chan interface{})
		go func(in, out chan interface{}) {
			defer wg.Done()
			defer close(out)

			c(in, out)
		}(in, out)
	}

	wg.Wait()
}

func getUserWorker(wg *sync.WaitGroup, users map[string]User, in, out chan interface{}) bool {
	defer wg.Done()

	var email interface{}
	var ok bool

	if email, ok = <-in; !ok {
		return false
	}

	user := GetUser(email.(string))
	if _, ok := users[user.Email]; !ok {
		users[user.Email] = user
		out <- user
	}

	return true
}

func SelectUsers(in, out chan interface{}) {
	// 	in - string
	// 	out - User
	//defer close(out)

	var users sync.Map
	wg := &sync.WaitGroup{}
	//var closed bool

	for email := range in {
		wg.Add(1)

		go func(out chan interface{}) {
			defer wg.Done()

			user := GetUser(email.(string))
			if _, ok := users.Load(user.Email); !ok {
				users.Store(user.Email, user)
				log.Println("sended user", user)
				out <- user
			}
		}(out)
	}

	wg.Wait()
}

func getMessagesWorker(wg *sync.WaitGroup, in, out chan interface{}) bool {
	defer wg.Done()
	var closed bool

	users := make([]User, GetMessagesMaxUsersBatch)
	for i := 0; i < GetMessagesMaxUsersBatch; i++ {
		user, ok := <-in
		if !ok {
			closed = true
			break
		}

		users[i] = user.(User)
	}

	msgs, _ := GetMessages(users...)
	for msg := range msgs {
		out <- msg
	}

	if closed {
		close(out)
		return false
	}

	return true
}

func SelectMessages(in, out chan interface{}) {
	// 	in - User
	// 	out - MsgID
	wg := &sync.WaitGroup{}
	opened := true

	for opened {
		users := make([]User, 0, GetMessagesMaxUsersBatch)
		for i := 0; i < GetMessagesMaxUsersBatch; i++ {
			user, ok := <-in
			if !ok {
				opened = false
				break
			}

			log.Println("readed user", user)
			users = append(users, user.(User))
		}

		if len(users) == 0 {
			break
		}

		wg.Add(1)
		go func(out chan interface{}) {
			defer wg.Done()

			msgs, _ := GetMessages(users...)
			for _, msg := range msgs {
				out <- msg
			}
		}(out)
	}

	wg.Wait()
}

const MaxParallelSpamRequests = 5

func CheckSpam(in, out chan interface{}) {
	// in - MsgID
	// out - MsgData

	//defer close(out)
	wg := &sync.WaitGroup{}

	for i := 0; i < MaxParallelSpamRequests; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			for msg := range in {
				res, err := HasSpam(msg.(MsgID))
				if err != nil {
					log.Println(err)
				} else {
					out <- MsgData{ID: msg.(MsgID), HasSpam: res}
				}
			}
		}()
	}

	wg.Wait()
}

func CombineResults(in, out chan interface{}) {
	// in - MsgData
	// out - string

	//defer close(out)
	var msgs []MsgData

	for msg := range in {
		msgs = append(msgs, msg.(MsgData))
	}

	slices.SortFunc(msgs, func(a, b MsgData) int {
		if a.HasSpam == true && b.HasSpam == false {
			return -1
		}

		if a.HasSpam == true && b.HasSpam == true {
			if a.ID < b.ID {
				return -1
			}
			return 1
		}

		if a.HasSpam == false && b.HasSpam == false {
			if a.ID < b.ID {
				return -1
			}

			return 1
		}

		return 1
	})

	for _, msg := range msgs {
		out <- fmt.Sprintf("%t %d", msg.HasSpam, msg.ID)
	}
}
