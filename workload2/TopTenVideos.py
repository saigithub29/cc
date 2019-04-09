
from pyspark import SparkContext
import argparse

"""
This module includes a few functions used in computing average rating per genre
"""
def extractVideos(record):
    """ This function converts entries of ratings.csv into key,value pair of the following format
    (movieID, rating)
    Args:
        record (str): A row of CSV file, with four columns separated by comma
    Returns:
        The return value is a tuple (movieID, genre)
    """
    try:
        video_id,trending_date,category_id,category,publish_time,views,likes,dislikes,comment_count,ratings_disabled,video_error_or_removed,country = record.split(",")
        likes = int(likes)
        dislikes = int(dislikes)
        video = [trending_date, category, likes, dislikes]
        return (video_id, country), [video]
    except:
        return ()



def calculDefference(line):
    key, value = line
    video_id, country = key
    category = value[0][1]
    growthDislike = (value[1][3]-value[0][3])-(value[1][2]-value[0][2])
    video = (video_id, category, country)
    return growthDislike, video


def mapTopTenVideos(line):
    growthDislike, video = line
    video_id = video[0]
    category = video[1]
    country = video[2]
    video = video_id + ",\t" + str(growthDislike) + ",\t" + category + ",\t" + country
    return video


if __name__ == "__main__":
    sc = SparkContext(appName="Average Rating per Genre")
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", help="the input path",
                        default='AllVideos_short.csv')
    parser.add_argument("--output", help="the output path",
                        default='videos_output')


    args = parser.parse_args()
    input_path = args.input
    output_path = args.output
    videoData = sc.textFile(input_path).map(extractVideos).filter(lambda x: x != ())
    groupedVideoData = videoData.reduceByKey(lambda x, y: x+y).filter(lambda x: len(x[1]) >= 2)
    calculGrowth = groupedVideoData.map(calculDefference).sortByKey(False).take(10)
    topTenRdd = sc.parallelize(calculGrowth, 1).map(mapTopTenVideos)
    topTenRdd.saveAsTextFile(output_path)
    
    summ = 0
    for line in topTenRdd.collect():
        summ += 1
        print(line)
        # if(summ==10): break




