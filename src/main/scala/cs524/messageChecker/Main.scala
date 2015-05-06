/**
 * Created by tshull7 on 5/5/15.
 */
package cs524.messageChecker

import java.io.File

import scala.collection.mutable.Map

case class MessageInfo(var firstRead: Long, var lastRead: Long, var firstWrite: Long, var lastWrite: Long){
  var obj: String = _
  var firstReadTime, lastReadTime, firstWriteTime, lastWriteTime:Long =  0
  def this(obj: String, firstRead: Long=0, lastRead: Long=0, firstWrite: Long=0, lastWrite: Long=0) = {
    this(firstRead, lastRead, firstWrite, lastWrite)
    this.obj = obj
  }
}

class MessageManager(actorName: String){
  val messageMap = Map[String, MessageInfo]()
  def checkForConflicts(other: MessageManager): Boolean = {
    var violation = false
    for((key, messageInfoOther) <- other.messageMap){
      if(messageMap.contains(key)){
        val MessageInfo(fr_1, lr_1, fw_1, lw_1) = messageMap(key)
        val MessageInfo(fr_2, lr_2, fw_2, lw_2) = messageInfoOther
        //val firstAccess_1 = if (fr_1 < fw_1) fr_1 else fw_1
        val lastAccess_1 = if (lr_1 > lw_1) lr_1 else lw_1
        //val firstAccess_2 = if (fr_2 < fw_2) fr_2 else fw_2
        val lastAccess_2 = if (lr_2 > lw_2) lr_2 else lw_2
        if(lastAccess_1 == 0 || lastAccess_2 == 0){
          //this means the there is a record of this object, but it was never used, which should not happen
          printf("there is a record of an object that was never accessed")
          throw new NullPointerException("object never accessed is in the record")
        }
        //this means that the object was accessed after an object was written by another thread
        if(lastAccess_1 > fw_2){
          violation = true
          //might want to put some kind of message here
        }
        if(lastAccess_2 > fw_1){
          violation = true
        }
      }
    }
    return violation
  }
  def recordEntry(obj: String): Unit ={
    if(! messageMap.contains(obj)){
      messageMap(obj) = new MessageInfo(obj)

    }else{
      println("SOMETHING IS MESSED UP HERE")
    }
  }

  def addRead(obj: String, num: Long, time: Long, first: Boolean = false): Unit = {
    if(first){
      messageMap(obj).firstRead = num
      messageMap(obj).firstReadTime = time
    }else{
      messageMap(obj).lastRead = num
      messageMap(obj).lastReadTime = time
    }
  }

  def addWrite(obj: String, num: Long, time: Long, first: Boolean = false): Unit = {
    if(first){
      messageMap(obj).firstWrite = num
      messageMap(obj).firstWriteTime = time
    }else{
      messageMap(obj).lastWrite = num
      messageMap(obj).lastWriteTime = time
    }
  }
}

object SystemOperations {
  var baseDir: String = _
  var numActors: Int = _
  var actorNames = Array[String]()
  var actorDirs = Array[String]()
  val messageMap = Map[String, MessageManager]()


  def scanFolders(base: String): Unit = {
    baseDir = base
    actorNames = (new java.io.File(baseDir).list().filter( suffix => new java.io.File(baseDir + "/" + suffix).isDirectory()))
    for(name <- actorNames){
      actorDirs = actorDirs.:+(baseDir + "/" + name)
    }
    numActors = actorDirs.size
  }

  def readInfoIntoMessageManagers(): Unit ={
    object ReadingState extends Enumeration {
      type FilePart = Value
      val Beginning, ObjectBeginningRead, FirstRead, Read, ObjectBeginningWrite, FirstWrite, Write = Value
    }
    import scala.io.Source
    import ReadingState._
    for(path <- actorDirs){
      var currState = Beginning
      var actorName = ""
      var currObject = ""
      for(line <- Source.fromFile(path + "/objectsTouched.txt").getLines()){
        currState match{
          case Beginning => {
            val beginningQuery = "Actor Name: (.+)".r
            line match{
              case beginningQuery(name) => {
                actorName = name
                if(! messageMap.contains(actorName)){
                  messageMap(actorName) = new MessageManager(actorName)
                }else{
                  println("Something may be wrong")
                }
                currState = ObjectBeginningRead
              }
              case _ => {}
            }
          }
          case ObjectBeginningRead => {
            val objectInfo = "log object: (.+), status: (.+)".r
            val messageSent = "Messages Sent".r
            line match {
              case objectInfo(obj, _) => {
                currObject = obj
                messageMap(actorName).recordEntry(currObject)
                currState = FirstRead
              }
              case messageSent() => { currState = ObjectBeginningWrite}
              case _ => {}
            }
          }
          case FirstRead => {
            val messageValue = "kind: (\\w+), num: (\\d+), time: (\\d+)".r
            val messageSent = "Messages Sent".r
            line match {
              case messageValue(kind, num, time) => {
                messageMap(actorName).addRead(currObject, num.toLong, time.toLong, first = true)
                currState = Read
              }
              case messageSent() => { currState = ObjectBeginningWrite }
              case _ => {}
            }
          }
          case Read => {
            val messageValue = "kind: (\\w+), num: (\\d+), time: (\\d+)".r
            val messageSent = "Messages Sent".r
            line match {
              case messageValue(kind, num, time) => {
                messageMap(actorName).addRead(currObject, num.toLong, time.toLong, first = false)
              }
              case messageSent() => { currState = ObjectBeginningWrite}
            }
          }
          case ObjectBeginningWrite => {
            val objectInfo = "log object: (.+), status: (.+)".r
            val objectDone = "End Object".r
            line match {
              case objectInfo(obj, _) => {
                messageMap(actorName).recordEntry(obj)
                currState = FirstWrite
              }
              case objectDone() => { currState = ObjectBeginningRead}
              case _ => {}
            }
          }
          case FirstWrite => {
            val messageValue = "kind: (\\w+), num: (\\d+), time: (\\d+)".r
            val objectDone = "End Object".r
            line match {
              case messageValue(kind, num, time) => {
                messageMap(actorName).addWrite(currObject, num.toLong, time.toLong, first = true)
                currState = Write
              }
              case objectDone() => { currState = ObjectBeginningRead }
              case _ => {}
            }
          }
          case Write => {
            val messageValue = "kind: (\\w+), num: (\\d+), time: (\\d+)".r
            val objectDone = "End Object".r
            line match {
              case messageValue(kind, num, time) => {
                messageMap(actorName).addWrite(currObject, num.toLong, time.toLong, first = false)
              }
              case objectDone() => { currState = ObjectBeginningRead }
              case _ => {}
            }
          }
        }
      }
    }
  }

  def checkForConflicts(): Boolean = {
    var violations = false
    for((k_1, v_1) <- messageMap; (k_2, v_2) <- messageMap){
      if(!v_1.checkForConflicts(v_2)){
        println("Actor %s conflicts with Actor %s".format(v_1, v_2))
        violations = true
      }
    }
    violations
  }
}

object Main{
  def main(args: Array[String]) = {
    val baseDir = args(0)
    SystemOperations.scanFolders(baseDir)
    SystemOperations.readInfoIntoMessageManagers()
    if(!SystemOperations.checkForConflicts()){
      println("PASS: everything checked out fine")
    }else {
      println("FAIL: this is no good")
    }
  }
}
