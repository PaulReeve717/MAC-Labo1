package ch.heig.mac;

import java.util.List;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.query.QueryOptions;
import com.couchbase.client.java.query.QueryResult;

import static com.couchbase.client.java.query.QueryOptions.queryOptions;


public class Requests {
    private final Cluster cluster;

    public Requests(Cluster cluster) {
        this.cluster = cluster;
    }

    public List<String> getCollectionNames() {
        QueryResult result = cluster.query(
                "SELECT RAW r.name\n" +
                        "FROM system:keyspaces r\n" +
                        "WHERE r.`bucket` = \"mflix-sample\";"
        );
        return result.rowsAs(String.class);
    }

    public List<JsonObject> inconsistentRating() {
        QueryResult result = cluster.query("Select imdb.id imdb_id, tomatoes.viewer.rating tomato_rating, imdb.rating imdb_rating\n" +
                "from `mflix-sample`._default.movies\n" +
                "where tomatoes.viewer.rating > 0 and ABS(tomatoes.viewer.rating - imdb.rating) > 7;");
        return result.rowsAsObject();
    }

    public List<JsonObject> topReviewers() {
        QueryResult result = cluster.query("Select name, COUNT(name) cnt\n" +
                "From `mflix-sample`._default.comments\n" +
                "Group by name\n" +
                "order by cnt DESC\n" +
                "Limit 10;");
        return result.rowsAsObject();
    }

    public List<String> greatReviewers() {
        QueryResult result = cluster.query("Select raw name\n" +
                "From `mflix-sample`._default.comments\n" +
                "Group by name\n" +
                "Having COUNT(name) > 300\n" +
                "order by COUNT(name) DESC;");
        return result.rowsAs(String.class);
    }

    public List<JsonObject> bestMoviesOfActor(String actor) {
        QueryResult result = cluster.query(
                "Select imdb.id id, imdb.rating rating, `cast`\n" +
                "From `mflix-sample`._default.movies\n" +
                "where imdb.rating > 9 and any actor in `cast` satisfies actor = $actor end;",
                queryOptions().parameters(JsonObject.create().put("actor", actor)));
       return result.rowsAsObject();
    }

    public List<JsonObject> plentifulDirectors() {
        QueryResult result = cluster.query("Select director_name, Count(movie._id) count_film\n" +
                "From `mflix-sample`._default.movies movie\n" +
                "Unnest directors director_name\n" +
                "Group by director_name\n" +
                "Having Count(movie._id) > 30");
        return result.rowsAsObject();

    }

    public List<JsonObject> confusingMovies() {
        QueryResult result = cluster.query("Select movie._id movie_id, movie.`title` `title`\n" +
                "From `mflix-sample`._default.movies movie\n" +
                "Where ARRAY_COUNT(directors) > 20");
        return result.rowsAsObject();
    }

    public List<JsonObject> commentsOfDirector1(String director) {
        QueryResult result = cluster.query("Select m._id movie_id,c.text text\n" +
                "From `mflix-sample`._default.movies m\n" +
                "Join `mflix-sample`._default.comments c ON m._id = c.movie_id\n" +
                "Where any director in m.directors satisfies director = $director end",
                queryOptions().parameters(JsonObject.create().put("director", director)));
        return result.rowsAsObject();
    }

    public List<JsonObject> commentsOfDirector2(String director) {
        QueryResult result = cluster.query("select c.movie_id,  c.text\n" +
                        "from `mflix-sample`._default.comments c\n" +
                        "where c.movie_id in (\n" +
                        "  select raw m._id\n" +
                        "  from `mflix-sample`._default.movies m\n" +
                        "  where any v in m.directors satisfies v = $director end\n" +
                        ")",
                queryOptions().parameters(JsonObject.create().put("director", director)));
        return result.rowsAsObject();
    }

    // Returns true if the update was successful.
    public Boolean removeEarlyProjection(String movieId) {
        cluster.query("update `mflix-sample`._default.theaters t1\n" +
                "set schedule = Array_flatten ((\n" +
                "select raw array v for v in schedule\n" +
                "  when v.movieId != $movieId\n" +
                "  or v.hourBegin > \"18:00:00\" end\n" +
                "from `mflix-sample`._default.theaters t2\n" +
                "where t2._id = t1._id\n" +
                "), 1)",
                queryOptions().parameters(JsonObject.create().put("movieId", movieId)));

        QueryResult check = cluster.query("select raw sched \n" +
                "  from `mflix-sample`._default.theaters\n" +
                "  unnest schedule sched\n" +
                "  where sched.movieId = $movieId\n" +
                "  and sched.hourBegin <= \"18:00:00\"",
                queryOptions().parameters(JsonObject.create().put("movieId", movieId)));
        return check.rowsAsObject().isEmpty();
    }

    public List<JsonObject> nightMovies() {
        QueryResult result = cluster.query("select m._id movie_id, m.`title` title\n" +
                "from `mflix-sample`._default.movies m\n" +
                "where m._id not in ( \n" +
                "  select distinct raw sched.movieId\n" +
                "  from `mflix-sample`._default.theaters\n" +
                "  unnest schedule sched\n" +
                "  where sched.hourBegin < \"18:00:00\"\n" +
                ")\n" +
                "  and m._id in (\n" +
                "    select distinct raw sched.movieId\n" +
                "    from `mflix-sample`._default.theaters\n" +
                "    unnest schedule sched\n" +
                "  )");
        return result.rowsAsObject();
    }


}
