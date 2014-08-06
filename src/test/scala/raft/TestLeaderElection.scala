package raft

import org.junit._
import java.util.concurrent._

class TestLeaderElection {
  @Test
  def testLeader = {
    println("Hello, World")
    Node.start
    TimeUnit.SECONDS.sleep(15)
    Node.stop
  }
}