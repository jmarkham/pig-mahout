register $udf_jar;

define recommendation com.hortonworks.pig.mahout.recommend.Recommendation(); 

users = load $users_file using com.hortonworks.pig.MultiDelimiterStorage('::') as (user_id:long);
user = filter users by user_id == $user_id;

ratings = load $ratings_file using com.hortonworks.pig.MultiDelimiterStorage('::') as (user_id:long, movie_id:long, rating:float, timestamp:long);
ratings_users = join ratings by user_id, user by user_id;

recommendations = foreach (group ratings_users by ratings.user_id) generate recommendation(ratings_users);

store recommendations into 'recommendations';