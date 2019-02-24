/**
  * Copyright (C) 2009-2013 Typesafe Inc. <http://www.typesafe.com>
  */
package actorbintree

import actorbintree.BinaryTreeNode.CopyFinished
import akka.actor._

import scala.collection.immutable.Queue
import scala.util.Random

object BinaryTreeSet {

  trait Operation {
    def requester: ActorRef
    def id: Int
    def elem: Int
  }

  trait OperationReply {
    def id: Int
  }

  /** Request with identifier `id` to insert an element `elem` into the tree.
    * The actor at reference `requester` should be notified when this operation
    * is completed.
    */
  case class Insert(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request with identifier `id` to check whether an element `elem` is present
    * in the tree. The actor at reference `requester` should be notified when
    * this operation is completed.
    */
  case class Contains(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request with identifier `id` to remove the element `elem` from the tree.
    * The actor at reference `requester` should be notified when this operation
    * is completed.
    */
  case class Remove(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request to perform garbage collection*/
  case object GC

  /** Holds the answer to the Contains request with identifier `id`.
    * `result` is true if and only if the element is present in the tree.
    */
  case class ContainsResult(id: Int, result: Boolean) extends OperationReply

  /** Message to signal successful completion of an insert or remove operation. */
  case class OperationFinished(id: Int) extends OperationReply

}

class BinaryTreeSet extends Actor {
  import BinaryTreeSet._
  import BinaryTreeNode._

  def createRoot: ActorRef =
    context.actorOf(BinaryTreeNode.props(0, initiallyRemoved = true))

  var root = createRoot

  // optional
  var pendingQueue = Queue.empty[Operation]

  // optional
  def receive: Receive = normal

  // optional
  /** Accepts `Operation` and `GC` messages. */
  val normal: Receive = {
    case GC  => context.become(garbageCollecting(createRoot))
    case msg => root ! msg
  }

  // optional
  /** Handles messages while garbage collection is performed.
    * `newRoot` is the root of the new binary tree where we want to copy
    * all non-removed elements into.
    */
  def garbageCollecting(newRoot: ActorRef): Receive = {
    case CopyFinished =>
      root = newRoot
      context.become(normal)
    case GC  =>
    case msg => newRoot ! msg
  }

}

object BinaryTreeNode {
  trait Position

  case object Left extends Position
  case object Right extends Position

  case class CopyTo(treeNode: ActorRef)
  case object CopyFinished

  def props(elem: Int, initiallyRemoved: Boolean) =
    Props(classOf[BinaryTreeNode], elem, initiallyRemoved)
}

class BinaryTreeNode(val elem: Int, initiallyRemoved: Boolean) extends Actor {
  import BinaryTreeNode._
  import BinaryTreeSet._

//  var subtrees: Map[Position, ActorRef] = Map[Position, ActorRef]()
  var left: Option[ActorRef] = None
  var right: Option[ActorRef] = None
  var removed: Boolean = initiallyRemoved

  // optional
  def receive: Receive = normal

  private def create(elem: Int): Option[ActorRef] =
    Option(
      context
        .actorOf(BinaryTreeNode.props(elem, initiallyRemoved = false))
    )

  private val insert: Receive = {
    case Insert(sender, id, insertElem) if insertElem == this.elem =>
      removed = false
      sender ! OperationFinished(id)
    case Insert(sender, id, insertElem)
        if insertElem < this.elem && left.isEmpty =>
      left = create(insertElem)
      sender ! OperationFinished(id)
    case msg @ Insert(_, _, insertElem) if insertElem < this.elem =>
      left.foreach(_ ! msg)
    case Insert(sender, id, insertElem)
        if insertElem > this.elem && right.isEmpty =>
      right = create(insertElem)
      sender ! OperationFinished(id)
    case msg @ Insert(_, _, insertElem) if insertElem > this.elem =>
      right.foreach(_ ! msg)
  }

  private val remove: Receive = {
    case Remove(sender, id, removeElem) if removeElem == this.elem =>
      removed = true
      sender ! OperationFinished(id)
    case msg @ Remove(_, _, removeElem)
        if removeElem < this.elem && left.isDefined =>
      left.foreach(_ ! msg)
    case msg @ Remove(_, _, removeElem)
        if removeElem > this.elem && right.isDefined =>
      right.foreach(_ ! msg)
    case Remove(sender, id, _) =>
      sender ! OperationFinished(id)
  }

  // optional
  /** Handles `Operation` messages and `CopyTo` requests. */
  val normal: Receive = insert orElse remove orElse {

    case msg @ Contains(sender, id, queryElem) =>
      if (queryElem == this.elem && !removed) {
        sender ! ContainsResult(id, result = true)
      } else if (queryElem < this.elem && left.isDefined) {
        left.foreach(_ ! msg)
      } else if (right.isDefined) {
        right.foreach(_ ! msg)
      } else {
        sender ! ContainsResult(id, result = false)
      }
    case msg @ CopyTo(treeNode) =>
      val insertId = Random.nextInt
      if (!removed)
        treeNode ! Insert(self, insertId, this.elem)
      val children: Set[ActorRef] = List(left, right).flatten.toSet
      children.foreach(_ ! msg)
      context.become(
        copying(
          children,
          insertConfirmed = false,
          insertId = insertId,
          sender()
        )
      )
  }

  // optional
  /** `expected` is the set of ActorRefs whose replies we are waiting for,
    * `insertConfirmed` tracks whether the copy of this node to the new tree has been confirmed.
    */
  def copying(expected: Set[ActorRef],
              insertConfirmed: Boolean,
              insertId: Int,
              requester: ActorRef): Receive = {
    case OperationFinished(`insertId`) =>
      context.become(
        copying(
          expected,
          insertConfirmed = true,
          insertId = insertId,
          requester
        )
      )
    case CopyFinished
        if expected.size == 1 && expected.contains(sender) && insertConfirmed =>
      requester ! CopyFinished
      self ! PoisonPill
    case CopyFinished =>
      context.become(
        copying(expected - sender(), insertConfirmed, insertId, requester)
      )

  }

}
