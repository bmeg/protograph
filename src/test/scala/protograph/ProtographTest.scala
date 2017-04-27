package protograph.test

import protograph._
import org.scalatest._

class ProtographTest extends FunSuite {
  val protograph = Protograph.loadProtograph("resources/test/bmeg.protograph.yaml")
  val variantAnnotation = """{"id": "variantAnnotation:variant:1:10521380:10521380:A:-:", "variantId": "variant:1:10521380:10521380:A:-", "transcriptEffects": [{"featureId": "gene:DFFA", "id": "transcriptEffect:gene:DFFA:variantAnnotation:variant:1:10521380:10521380:A:-::-", "alternateBases": "-", "effects": [{"term": "DEL"}]}]}"""

  test("parse protograph") {
    assert(protograph.transforms.size == 19)
  }

  test("lifting fields in embedded terminals") {
    println(variantAnnotation)
    protograph.processMessage(protograph.printEmitter) ("VariantAnnotation") (Protograph.readJSON(variantAnnotation))
  }
}

