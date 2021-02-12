object VideoData {
  def extract(row:String) = {
    def getLong(str:String): Long = if(!str.equals("view_count") && !str.equals("categoryId")) str.toLong else -1
    def getInt(str:String): Int = if(!str.equals("view_count") && !str.equals("categoryId")) str.toInt else -1
    val columns = row.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1)
      .map(_.replaceAll("\"",""))
    VideoData(
      columns(0),
      columns(1),
      columns(2),
      columns(3),
      columns(4),
      getInt(columns(5)),
      columns(6),
      columns(7),
      getLong(columns(8)),
      columns(9),
      columns(10),
      columns(11),
      columns(12),
      columns(13),
      columns(14),
      columns(15)
    )
  }
}

case class VideoData(
                      video_id:String,
                      title:String,
                      publishedAt:String,
                      channelId:String,
                      channelTitle:String,
                      categoryId:Int,
                      trending_date:String,
                      tags:String,
                      view_count:Long,
                      likes:String,
                      dislikes:String,
                      comment_count:String,
                      thumbnail_link:String,
                      comments_disabled:String,
                      ratings_disabled:String,
                      description:String
                    )
