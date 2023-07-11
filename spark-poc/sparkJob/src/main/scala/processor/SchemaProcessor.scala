package processor

import model.{BaseModel, pJson}
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.StructType

object SchemaProcessor {

  def caseClass2Struct[T](schemaClass: BaseModel): Any = {
    // TODO : fix this
    ScalaReflection.schemaFor[pJson].dataType.asInstanceOf[StructType]
  }

}
