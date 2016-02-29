package de.frosner.ddq.core


import de.frosner.ddq.constraints.{DummyConstraint, ConstraintFailure, ConstraintSuccess}
import de.frosner.ddq.reporters.Reporter
import org.apache.spark.sql.DataFrame
import org.apache.spark.storage.StorageLevel
import org.mockito.Mockito._
import org.scalatest.mock.MockitoSugar
import org.scalatest.{FlatSpec, Matchers}

class RunnerTest extends FlatSpec with Matchers with MockitoSugar {

  "A runner" should "run with multiple checks" in {
    val df1 = mock[DataFrame]
    val df2 = mock[DataFrame]

    val message1 = "1"
    val status1 = ConstraintSuccess
    val constraint1 = DummyConstraint(message1, status1)
    val result1 = constraint1.fun(df1)

    val message2 = "2"
    val status2 = ConstraintFailure
    val constraint2 = DummyConstraint(message2, status2)
    val result2 = constraint2.fun(df2)

    val check1 = Check(df1, None, None, Seq(constraint1))
    val check2 = Check(df2, None, None, Seq(constraint2))

    val checkResults = Runner.run(List(check1, check2), List.empty)

    checkResults.size shouldBe 2

    val checkResult1 = checkResults(check1)
    val checkResult2 = checkResults(check2)

    checkResult1.check shouldBe check1
    checkResult1.constraintResults shouldBe Map((constraint1, result1))

    checkResult2.check shouldBe check2
    checkResult2.constraintResults shouldBe Map((constraint2, result2))
  }

  it should "persist and unpersist the data frame if a persist method is specified" in {
    val storageLevel = StorageLevel.MEMORY_AND_DISK

    val df = mock[DataFrame]
    when(df.persist(storageLevel)).thenReturn(df.asInstanceOf[df.type])

    val check = Check(df, None, Some(storageLevel), Seq(DummyConstraint("test", ConstraintSuccess)))
    val checkResult = Runner.run(List(check), List.empty)(check)

    verify(df).persist(storageLevel)
    verify(df).unpersist()
  }

  it should "not persist and unpersist the data frame if no persist method is specified" in {
    val df = mock[DataFrame]

    val check = Check(df, None, None, Seq(DummyConstraint("test", ConstraintSuccess)))
    val checkResult = Runner.run(List(check), List.empty)(check)

    verify(df, never()).persist()
    verify(df, never()).unpersist()
  }

  it should "report to all reporters what it returns" in {
    val df = mock[DataFrame]

    val check = Check(df, None, None, Seq(DummyConstraint("test", ConstraintSuccess)))
    val checkResult = Runner.run(List(check), List.empty)(check)

    val reporter1 = mock[Reporter]
    val reporter2 = mock[Reporter]

    Runner.run(List(check), List(reporter1, reporter2))
    verify(reporter1).report(checkResult)
    verify(reporter2).report(checkResult)
  }

}
