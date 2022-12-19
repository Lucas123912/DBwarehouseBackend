package cn.edu.tongji.dwhivebackend.Service.Impl;

import cn.edu.tongji.dwhivebackend.DTO.MovieInfoDto;
import cn.edu.tongji.dwhivebackend.Service.HiveService;
import org.jcodings.util.Hash;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * TODO:此处写HiveServiceImpl类的描述
 *
 */

@Service
public class HiveServiceImpl implements HiveService {

    @Autowired
    @Qualifier("jdbcTemplate")
    private JdbcTemplate jdbcTemplate;


    @Override
    public List<String> getMovieNameByString(String movieString) {
        String sql = "select movie_title from movie where locate(\"" + movieString + "\",movie_title) = 1";
        List<String> result = new ArrayList<>();
        List<Map<String, Object>> list = jdbcTemplate.queryForList(sql);
        for (Map<String, Object> item : list) result.add((String) item.get("movie_title"));
        return result;
    }

    @Override
    public List<String> getDirectorNameByString(String directorString) {
        String sql = "select name from person where locate(\"" + directorString + "\",person.name) = 1 and occupation = \"director\"";
        List<String> result = new ArrayList<>();
        List<Map<String, Object>> list = jdbcTemplate.queryForList(sql);
        for (Map<String, Object> item : list) result.add((String) item.get("name"));
        return result;
    }

    @Override
    public List<String> getActorNameByStr(String actorString) {
        String sql = "select name from person where locate(\"" + actorString + "\",person.name) = 1 and occupation = \"actor\"";
        List<String> result = new ArrayList<>();
        List<Map<String, Object>> list = jdbcTemplate.queryForList(sql);
        for (Map<String, Object> item : list) result.add((String) item.get("name"));
        return result;
    }

    @Override
    public List<String> getCategoryNameByStr(String category) {
        String sql = "select style from movie where locate(\"" + category + "\",style) = 1";
        List<String> result = new ArrayList<>();
        List<Map<String, Object>> list = jdbcTemplate.queryForList(sql);
        for (Map<String, Object> item : list) result.add((String) item.get("style"));
        return result;
    }

    @Override
    public List<String> getAllDirectorsByMovieAsin(String movieAsin) {
        String sql = "select directors from movie where asin = \"" + movieAsin + "\"";
        List<String> result = new ArrayList<>();
        List<Map<String, Object>> list = jdbcTemplate.queryForList(sql);
        String str = (String) list.get(0).get("directors");
        if (str.equals("[]")) return null;
        str = str.substring(1, str.length() - 1);
        for (String temp : str.split(",")) {
            result.add(temp.substring(1, temp.length() - 1));
        }
        return result;
    }


    @Override
    public List<String> getAllActorsByMovieAsin(String movieAsin) {
        String sql = "select actors from movie where asin = \"" + movieAsin + "\"";
        List<String> result = new ArrayList<>();
        List<Map<String, Object>> list = jdbcTemplate.queryForList(sql);
        String str = (String) list.get(0).get("actors");
        if (str.equals("[]")) return null;
        str = str.substring(1, str.length() - 1);
        for (String temp : str.split(",")) {
            result.add(temp.substring(1, temp.length() - 1));
        }
        return result;
    }

    @Override
    public HashMap<String, Object> getMaxCooperationTimeOfActors() {
//        String sql = "select * from actor_actor where movie_count = (select max(movie_count) from actor_actor)";
        String sql = "select * from actor_actor order by movie_count desc limit 1";
        HashMap<String, Object> result = new HashMap<>();
        List<Map<String, Object>> list = jdbcTemplate.queryForList(sql);
        List<String> actors = new ArrayList<>();
        actors.add((String) list.get(0).get("actor_actor.first_actor_name"));
        actors.add((String) list.get(0).get("actor_actor.second_actor_name"));
        result.put("actor", actors);
        result.put("number", list.get(0).get("actor_actor.movie_count"));
        return result;
    }

    @Override
    public HashMap<String, Object> getMaxCooperationTimeOfDirectors() {
        String sql = "select * from director_director where movie_count = (select max(movie_count) from director_director)";
//        String sql = "select * from director_director order by movie_count desc limit 1";
        HashMap<String, Object> result = new HashMap<>();
        List<Map<String, Object>> list = jdbcTemplate.queryForList(sql);
        List<String> directors = new ArrayList<>();
        directors.add((String) list.get(0).get("director_director.first_director_name"));
        directors.add((String) list.get(0).get("director_director.second_director_name"));
        result.put("director", directors);
        result.put("number", list.get(0).get("director_director.movie_count"));
        System.out.println(list);
        return result;
    }

    @Override
    public HashMap<String, Object> getMaxCooperationTimeOfActorsAndDirectors() {
//        String sql = "select * from actor_director where movie_count = (select max(movie_count) from actor_director)";
        String sql = "select * from actor_director order by movie_count desc limit 1";
        HashMap<String, Object> result = new HashMap<>();
        List<Map<String, Object>> list = jdbcTemplate.queryForList(sql);
        result.put("actor", list.get(0).get("actor_director.actor_name"));
        result.put("director", list.get(0).get("actor_director.director_name"));
        result.put("number", list.get(0).get("actor_director.movie_count"));
        return result;
    }

    @Override
    public HashMap<String, Object> getMovieResultsByMutipleRules(MovieInfoDto movieInfoDto) {
        String sql = "select * from movie where ";
        long startTime = System.currentTimeMillis();
        boolean isFirst = true;
        if (movieInfoDto.getMovieName() != null) {
            sql = sql + ("movie_title = \"" + movieInfoDto.getMovieName() + "\" ");
            isFirst = false;
        }

        if (movieInfoDto.getCategory() != null) {
            sql = isFirst ? sql : (sql + "and ");
            isFirst = false;
            sql = sql + ("style = \"" + movieInfoDto.getCategory() + "\" ");
        }

        if (movieInfoDto.getDirectorNames() != null) {
            for (String item : movieInfoDto.getDirectorNames()) {
                sql = isFirst ? sql : (sql + "and ");
                isFirst = false;
                sql = sql + ("array_contains(directors,\"" + item + "\") ");
            }
        }


        if (movieInfoDto.getActors() != null) {
            for (String item : movieInfoDto.getActors()) {
                sql = isFirst ? sql : (sql + "and ");
                isFirst = false;
                sql = sql + ("array_contains(actors,\"" + item + "\") ");
            }
        }

        if (movieInfoDto.getMinScore()!=null ) {
            sql = isFirst ? sql : (sql + "and ");
            isFirst = false;
            sql += ("score > " + movieInfoDto.getMinScore() + " ");
        }

        if (movieInfoDto.getMaxScore() != null ){
            sql = isFirst ? sql : (sql + "and ");
            isFirst = false;
            sql += ("score < " + movieInfoDto.getMaxScore() + " ");
        }

        if (movieInfoDto.getMinYear() != null) {
            sql = isFirst ? sql : (sql + "and ");
            isFirst = false;
            sql += ("year*10000+month*100+day < " + (10000*movieInfoDto.getMaxYear()+100*movieInfoDto.getMaxMonth()+movieInfoDto.getMaxDay()) + " and year*10000+month*100+day > " + (10000*movieInfoDto.getMinYear()+100*movieInfoDto.getMinMonth()+movieInfoDto.getMinDay()));
            System.out.println(sql);
        }

        List<Map<String, Object>> list = jdbcTemplate.queryForList(sql);
        long endTime = System.currentTimeMillis();
        HashMap<String, Object> result = new HashMap<>();

        List<HashMap<String, Object>> movieResult = new ArrayList<>();
        for (Map<String, Object> item : list) {
            HashMap<String, Object> node = new HashMap<>();
            String sql1 = "select relative_id from relationship where product_id=\""+item.get("movie.asin")+"\"";
            List<String> result1=new ArrayList<>();
            List<Map<String, Object>> list1 = jdbcTemplate.queryForList(sql1);

            if(list1.size()==0)
            {
                node.put("brothers","[]");
                node.put("edition",1);
            }
            else {
                for (int i=0;i<list1.size();i++) {
                    String temp= (String) list1.get(i).get("relative_id");
                    result1.add(temp);
                }
                node.put("brothers", result1);
                node.put("edition", list1.size() + 1);
            }
            node.put("asin", item.get("movie.asin"));
            node.put("title", item.get("movie.movie_title"));
            node.put("category", item.get("movie.style"));
            node.put("score", item.get("movie.score"));
            node.put("commentNum", item.get("movie.comment_num"));
            node.put("runTime", item.get("movie.run_time"));
            node.put("showDate",item.get("movie.release_date"));
            System.out.println(node);
            movieResult.add(node);
        }
        result.put("movies", movieResult);
        result.put("movieNum", list.size());
        result.put("time", endTime - startTime);

        return result;
    }


}

