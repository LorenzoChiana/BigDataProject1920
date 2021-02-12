object CategoryData {
  def extract(row: String) = {
    def getInt(str:String): Int = str.toInt
    val columns = row.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1)
      .map(_.replaceAll("\"",""))
      .map(_.replaceAll("\\{", ""))
      .map(_.replaceAll("\\}", ""))
      .map(_.replaceAll("id:", ""))
      .map(_.replaceAll("category:", ""))
    CategoryData(getInt(columns(0)), columns(1))
  }
}

case class CategoryData(id: Int, category: String)