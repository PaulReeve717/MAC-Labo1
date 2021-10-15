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
        //TODO créér index
        QueryResult result = cluster.query("Select m._id, c.text\n" +
                "From `mflix-sample`._default.movies m\n" +
                "Join `mflix-sample`._default.comments c On m._id = c.movie_id");
        throw new UnsupportedOperationException("Not implemented, yet");
    }

    public List<JsonObject> commentsOfDirector2(String director) {
        throw new UnsupportedOperationException("Not implemented, yet");
    }

    // Returns true if the update was successful.
    public Boolean removeEarlyProjection(String movieId) {
        throw new UnsupportedOperationException("Not implemented, yet");
    }

    public List<JsonObject> nightMovies() {
        throw new UnsupportedOperationException("Not implemented, yet");
    }


}
