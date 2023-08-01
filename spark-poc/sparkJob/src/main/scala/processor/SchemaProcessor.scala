package processor

import model.{BaseModel, PJson}
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.StructType

object SchemaProcessor {

  def caseClass2Struct[T](schemaClass: BaseModel): Any = {
    // TODO : fix this
    ScalaReflection.schemaFor[PJson].dataType.asInstanceOf[StructType]
  }

}
