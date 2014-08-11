package raft

case class Entry[T]( command: T, term: Int, index: Int )