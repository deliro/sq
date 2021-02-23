package main

import (
	"testing"
	"time"
)

const (
	TypeNone = 0
	TypePp   = 1 << iota
	TypeU
	TypePartner
	TypeRetail
)

func TestAccountList_GetByFlags(t *testing.T) {
	list := NewAccountList(1)
	list.Put(NewAccount("none", "", TypeNone))
	list.Put(NewAccount("pp", "", TypePp))
	list.Put(NewAccount("u", "", TypeU))
	list.Put(NewAccount("partner", "", TypePartner))
	list.Put(NewAccount("retail", "", TypeRetail))
	list.Put(NewAccount("pp&partner", "", TypePp|TypePartner))

	first := list.GetByFlags(TypePartner | TypePp).Email
	second := list.GetByFlags(TypePartner).Email
	if first != "pp&partner" || second != "partner" {
		t.Fatal("Wrong result")
	}

	for i := 0; i < len(list.free); i++ {
		list.GetByFlags(TypeNone)
	}
}

func TestPriority(t *testing.T) {
	list := NewAccountList(1)
	emailGetter := make(chan struct{})
	flagsGetter := make(chan struct{})
	go func() {
		list.GetByFlags(TypeRetail)
		flagsGetter <- struct{}{}
	}()

	go func() {
		list.GetByEmail("retail")
		emailGetter <- struct{}{}
	}()

	go func() {
		time.Sleep(time.Millisecond * 10)
		list.Put(NewAccount("retail", "", TypeRetail))
	}()

	select {
	case <-emailGetter:
		return
	case <-flagsGetter:
		t.Fatal("Email must get first")
	}
}
