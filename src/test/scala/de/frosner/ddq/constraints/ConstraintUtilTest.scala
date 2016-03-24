package de.frosner.ddq.constraints

import org.scalatest.{FlatSpec, Matchers}

import scala.util.{Failure, Success}

class ConstraintUtilTest extends FlatSpec with Matchers {

  "tryToStatus" should "return success if the condition is met and the try was a success" in {
    val tryObject = Success(5)
    ConstraintUtil.tryToStatus(tryObject, (i: Int) => i == 5) shouldBe ConstraintSuccess
  }

  it should "return failure if the condition is not met and the try was a success" in {
    val tryObject = Success(5)
    ConstraintUtil.tryToStatus(tryObject, (i: Int) => i == 4) shouldBe ConstraintFailure
  }

  it should "return error if the try was a failure" in {
    case object DummyException extends Throwable
    val tryObject = Failure(DummyException)
    ConstraintUtil.tryToStatus(tryObject, (i: Int) => i == 5) shouldBe ConstraintError(DummyException)
  }

}
