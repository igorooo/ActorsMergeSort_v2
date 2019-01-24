/**Igor Czeczot */

import akka.actor.{Actor, ActorLogging, ActorRef, ActorSystem, PoisonPill, Props}


/**
  * MergeSort - Actor working as employer of Sorters
  *             Sorters -Actors - sorting received sub arrays with merge sort, and sending them back to sender()
  *
  * @param ARRAY  array to sort
  * @param ACTORS_NO how many actors should be involved in sorting ARRAY
  */


class MergeSort(ARRAY : Array[Int], var ACTORS_NO : Int) extends Actor {


  /** CONSTRUCTOR */

  val LEN = ARRAY.length
  var FINAL_ARRAY = new Array[Int](0)

  var ACTOR_IX = 0 //actor index

  if( ACTORS_NO > (LEN / MergeSort.MAX_ACTORS_DIVIDER) ){   //Maksymalna liczba aktorów to length(ARRAY) / MergeSort.MAX_ACTORS_DIVIDER (3)
    this.ACTORS_NO = LEN / MergeSort.MAX_ACTORS_DIVIDER
    if(ACTORS_NO == 0) this.ACTORS_NO += 1
  }

  this.ACTORS_NO += 1

  val SORTERS = new Array[ActorRef](ACTORS_NO)

  for(i <- 0 until ACTORS_NO){
    SORTERS(i) = context.actorOf(Sorter.props)
  }

  /** end of CONSTRUCTOR */


  /** recaive funct:
    *
    * To start work of actor, send him object MergeSort.Start
    *
    * If Array[Int] received, then merge this with current FINAL_ARRAY
    * (Sub actors are sending messeges with (Array[Int]) as result of their work)
    *
    * if length of FINAL_ARRAY is equal to length of ARRAY(primary array given in constructor)
    *   then it means that all actors have finished their work
    *
    *
    */
  override def receive: Receive = {

    case MergeSort.Start => {
      start()
    }

    case array : Array[Int] => {

      FINAL_ARRAY = mergev2(FINAL_ARRAY,array)

      if(FINAL_ARRAY.length == LEN){
        println("Sorted Array:")
        for(i <- 0 until LEN) print(s"${FINAL_ARRAY(i)} ")
        println()
        //context.system.terminate()
        poison()
      }
    }

    case msg => {
      println(s"Unexpected message: $msg")
    }
  }

  def poison() = {
    for(i <- 0 until ACTORS_NO)
      SORTERS(i) ! PoisonPill
    self ! PoisonPill
  }



  def next_actor(): ActorRef = {
    ACTOR_IX = (ACTOR_IX+1)%ACTORS_NO
    SORTERS(ACTOR_IX)
  }

  def start() = {

    var IX = 0
    var IXX = 0  //iteratory

    val NUM_FOR_ACTOR = LEN / ACTORS_NO
    var T_IX = 0

    while(IX < LEN){
      if(T_IX >= NUM_FOR_ACTOR){
        next_actor() ! ARRAY.slice(IXX, IX)  //komunikat
        T_IX = 0
        IXX = IX
      }
      else{
        T_IX +=1
      }
      IX += 1
    }
    next_actor() ! ARRAY.slice(IXX,LEN)  //komunikat
  }


  def mergev2(ARRAY_1 : Array[Int], ARRAY_2 : Array[Int] ):Array[Int] = {

    val LEN_1 = ARRAY_1.length
    val LEN_2 = ARRAY_2.length

    val N_ARR = new Array[Int](LEN_1 + LEN_2 )

    var I_1 = 0  // iterator dla ARRAY_1
    var I_2 = 0  // -----.------ ARRAY_2
    var I_3 = 0   //-----.------ N_ARR

    while(I_1 < LEN_1  &&  I_2 < LEN_2){

      if(ARRAY_1(I_1) <= ARRAY_2(I_2)){
        N_ARR(I_3) = ARRAY_1(I_1)
        I_1 += 1
      }

      else{
        N_ARR(I_3) = ARRAY_2(I_2)
        I_2 += 1
      }

      I_3 += 1
    }

    while(I_1 < LEN_1){
      N_ARR(I_3) = ARRAY_1(I_1)
      I_1 += 1
      I_3 += 1
    }

    while(I_2 < LEN_2){
      N_ARR(I_3) = ARRAY_2(I_2)
      I_2 += 1
      I_3 += 1
    }

    N_ARR
  }
}

object MergeSort{
  def props(arr:Array[Int], no : Int) = Props(classOf[MergeSort],arr,no)
  val MAX_ACTORS_DIVIDER = 3
  case object Start
}



class Sorter() extends Actor{


  override def receive: Receive = {

    case array: Array[Int] => {

      val ARR = array.clone()

      merge_sort(ARR,0, ARR.length-1)

      sender() ! ARR

    }
  }

  def merge_sort(ARRAY : Array[Int], LO : Int, UP : Int):Unit = {

    if(LO < UP){
      var MID = (LO + UP) / 2

      merge_sort(ARRAY,LO,MID)
      merge_sort(ARRAY,MID+1,UP)

      merge(ARRAY,LO,MID,UP)
    }

  }

  def merge(ARRAY : Array[Int], LO : Int, MID : Int, UP : Int ):Unit = {

    val LEN_1 = MID - LO +1
    val LEN_2 = UP - MID

    var ARRAY_1 = new Array[Int](LEN_1+1)
    var ARRAY_2 = new Array[Int](LEN_2+1)


    for(i <- 0 until LEN_1)
      ARRAY_1(i) = ARRAY(LO + i)

    for(i <- 0 until LEN_2)
      ARRAY_2(i) = ARRAY(MID + i + 1)

    var I_1 = 0  // iterator dla ARRAY_1
    var I_2 = 0  // -----.------ ARRAY_2
    var I_3 = LO   //-----.------ N_ARR

    while(I_1 < LEN_1  &&  I_2 < LEN_2){

      if(ARRAY_1(I_1) <= ARRAY_2(I_2)){
        ARRAY(I_3) = ARRAY_1(I_1)
        I_1 += 1
      }

      else{
        ARRAY(I_3) = ARRAY_2(I_2)
        I_2 += 1
      }

      I_3 += 1
    }

    while(I_1 < LEN_1){
      ARRAY(I_3) = ARRAY_1(I_1)
      I_1 += 1
      I_3 += 1
    }

    while(I_2 < LEN_2){
      ARRAY(I_3) = ARRAY_2(I_2)
      I_2 += 1
      I_3 += 1
    }
  }

}

object Sorter{
  def props = Props(classOf[Sorter])
}

object Main extends App {

  val ourSystem = ActorSystem()


  val no_actors = 3

  val arr1 = Array(7,9,8,9,2,1,11,4,2,8,5,6,3,6)
  var mergesort1 : ActorRef = ourSystem.actorOf(MergeSort.props(arr1,no_actors))

  val arr2 = Array(3,2,1,0,-1,-2)
  var mergesort2 : ActorRef = ourSystem.actorOf(MergeSort.props(arr2,2))

  val arr3 = Array(7,9,8,9,2,1,11,4,2,8,5,6,3,6,-2,-3,1,2,3,12,3123,23,34,-42,-5654,312,424,53452,242,2342,535,234234,5345,-234,23)
  var mergesort3 : ActorRef = ourSystem.actorOf(MergeSort.props(arr3,6))

  mergesort1 ! MergeSort.Start
  Thread.sleep(1000)

  mergesort2 ! MergeSort.Start
  Thread.sleep(1000)

  mergesort3 ! MergeSort.Start
  Thread.sleep(1000)



  Thread.sleep(2000)
  ourSystem.terminate()
}


