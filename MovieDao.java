package fr.isen.java2.db.daos;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

import fr.isen.java2.db.entities.Genre;
import fr.isen.java2.db.entities.Movie;

import static fr.isen.java2.db.daos.DataSourceFactory.getDataSource;

public class MovieDao {

	public List<Movie> listMovies() {
		List<Movie> listOfMovies = new ArrayList<>();
		try (Connection connection = getDataSource().getConnection()) {
			try (Statement statement = connection.createStatement()) {
				try (ResultSet results = statement.executeQuery("SELECT * FROM movie JOIN genre ON movie.genre_id = genre.idgenre")) {
					while (results.next()) {
						Movie movie = new Movie(
								results.getInt("idmovie"),
								results.getString("title"),
								results.getDate("release_date").toLocalDate(),
							    new Genre(results.getInt("idgenre"), results.getString("name")),
								results.getInt("duration"),
								results.getString("director"),
								results.getString("summary"));
						listOfMovies.add(movie);
					}
				}
			}
			return listOfMovies;
		} catch (SQLException e) {
			throw new RuntimeException("Error while listing movies", e);
		}
	}


	public List<Movie> listMoviesByGenre(String genreName) {
		List<Movie> listOfMovies = new ArrayList<>();
		try (Connection connection = getDataSource().getConnection()) {
			try (PreparedStatement statement = connection.prepareStatement(
					"SELECT * FROM movie JOIN genre ON movie.genre_id = genre.idgenre WHERE genre.name = ?")) {
				statement.setString(1, genreName); // si jamais on veux mettre comdy, il faut mettre cette ligne en commentaire et la partie drama de MovieDaoTest pour eviter une erreur
				try (ResultSet results = statement.executeQuery()) {
					while (results.next()) {
						Movie movie = new Movie(
								results.getInt("idmovie"),
								results.getString("title"),
								results.getDate("release_date").toLocalDate(),
								new Genre(results.getInt("idgenre"), results.getString("name")),
								results.getInt("duration"),
								results.getString("director"),
								results.getString("summary"));
						listOfMovies.add(movie);
					}
				}
			}
			return listOfMovies;
		} catch (SQLException e) {
			throw new RuntimeException("Error while listing movies by genre", e);
		}
	}
	public Movie addMovie(Movie movie) {
		try(Connection connection = getDataSource().getConnection()){
			String sqlQuery = "INSERT INTO movie(title,release_date,genre_id,duration,director,summary) VALUES(?,?,?,?,?,?)";
			try(PreparedStatement statement = connection.prepareStatement(sqlQuery, Statement.RETURN_GENERATED_KEYS)){
				statement.setString(1,movie.getTitle());
				statement.setString(2, movie.getReleaseDate().toString());
				statement.setInt(3, movie.getGenre().getId());
				statement.setInt(4, movie.getDuration());
				statement.setString(5, movie.getDirector());
				statement.setString(6, movie.getSummary());
				statement.executeUpdate();
				ResultSet ids = statement.getGeneratedKeys();
				if(ids.next()){
					return new Movie(ids.getInt(1), movie.getTitle(), movie.getReleaseDate(), movie.getGenre(),movie.getDuration(), movie.getDirector(), movie.getSummary());
				}
				else {
					throw new RuntimeException("Error while adding movie");
				}
			}
		}
		catch (SQLException e){
			throw new RuntimeException("Error creating the statement", e);
		}
	}
}
