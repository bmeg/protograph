package protograph.test

import protograph._
import org.scalatest._

class ProtographTest extends FunSuite {
  test("parse protograph") {
    val protograph = Protograph.loadProtograph("resources/test/bmeg.protograph.yaml")
    assert(protograph.transforms.size == 19)
  }
}
