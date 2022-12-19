package cn.edu.tongji.dwbackend.neo4j.controller;

import cn.edu.tongji.dwbackend.dto.MovieInfoDto;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.neo4j.driver.Session;
import org.neo4j.driver.internal.value.NullValue;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * MovieController类
 */

@RestController
@RequestMapping("/neo4j/movie")
public class MovieController {
    private final Driver driver;

    public MovieController(Driver driver){
        this.driver = driver;
    }

    @RequestMapping(method = RequestMethod.POST)
    public HashMap<String,Object> getMovieByCondition(@RequestBody  MovieInfoDto movieInfo) {

        // match (m:Movie), (m)-[:Belong]->(c:Category{name:'DTS'}) where m.title = 'Book and Sword' return count(m)
        // where m.year*10000+m.month*100+m.day >=20101102
        // 导演：match (m:Movie), (m)<-[:MainAct]-(:Person{name:'Santana'}),(m)<-[:MainAct]-(:Person{name:'Treglia'})
        // 评分用 where
        try (Session session = driver.session()) {
            String query = "match (m:Movie) ";
            if (movieInfo.getCategory() != null){
                query +=" , (m)-[:Belongs]->(:Genre{gene_name:\""+movieInfo.getCategory()+"\"}) ";
            }

            // 导演名称
            if(movieInfo.getDirectorNames() != null){
                for(String directorName: movieInfo.getDirectorNames()){
                    query += " ,(m)<-[:Direct]-(:Director{director_name:\""+directorName+"\"})";
                }
            }


            // 演员名称
            if(movieInfo.getActors() != null){
                for(String actor: movieInfo.getActors()){
                    query += " ,(m)<-[:Act]-(:Actor{actor_name:\""+actor+"\"})";
                }
            }

            Boolean whereAppear = false;
            // 电影名称
            if(movieInfo.getMovieName() != null){
                query += " where m.movie_name=\""+movieInfo.getMovieName()+"\" ";
                whereAppear = true;
            }

            // 最低评分
            if (movieInfo.getMinScore() != null){
                if (whereAppear){
                    query+= " and ";
                }
                else {
                    query += " where ";
                    whereAppear = true;
                }
                query += " m.movie_score >="+ movieInfo.getMinScore()+" ";
            }

            // 最高评分
            if (movieInfo.getMaxScore() != null){
                if (whereAppear){
                    query+= " and ";
                }
                else {
                    query += " where ";
                    whereAppear = true;
                }
                query+=" m.movie_score <= "+movieInfo.getMaxScore()+" ";
            }


            // 上映时间
            if(movieInfo.getMinYear()!=null){
                if (whereAppear){
                    query+= " and ";
                }
                else {
                    query += " where ";
                    whereAppear = true;
                }
                query+=" m.year*10000+m.month*100+m.day >= "+
                        (10000*movieInfo.getMinYear()+100*movieInfo.getMinMonth()+movieInfo.getMinDay())+" ";
            }
            if(movieInfo.getMaxYear()!=null){
                if (whereAppear){
                    query+= " and ";
                }
                else {
                    query += " where ";
                    whereAppear = true;
                }
                query+=" m.year*10000+m.month*100+m.day <= "+
                        (10000*movieInfo.getMaxYear()+100*movieInfo.getMaxMonth()+movieInfo.getMaxDay())+" ";
            }


            query+=" return m ";
            System.out.println("查询语句为: "+query);

            // 记录开始时间
            long startTime = System.currentTimeMillis();
            Result res=
                    session.run(query);




            HashMap<String,Object> response = new HashMap<>();

            List<Record> result = res.list();

            List<HashMap<String, Object>> movieResult = new ArrayList<>();
            // 记录结束时间
            long endTime = System.currentTimeMillis();
            // 返回50条
            for(int i=0;i<result.size() && i <50;++i){
                HashMap<String, Object> movieNode = new HashMap<>();
                if (result.get(i).get(0).get("product_id") != NullValue.NULL){
                    movieNode.put("asin",result.get(i).get(0).get("product_id").asString());
                }
                if (result.get(i).get(0).get("movie_name") != NullValue.NULL){
                    movieNode.put("title",result.get(i).get(0).get("movie_name").asString());
                }
                if (result.get(i).get(0).get("day_str") != NullValue.NULL){
                    movieNode.put("time",result.get(i).get(0).get("day_str").asString());
                }
                movieResult.add(movieNode);
            }

            response.put("movies",movieResult);
            response.put("movieNum",result.size());
            System.out.println(result.size());
            response.put("time",(endTime-startTime));

            return response;
        }
    }

    @GetMapping(path = "/name", produces = MediaType.APPLICATION_JSON_VALUE)
    public HashMap<String, Object> findMovieByName(@RequestParam String name){
        try (Session session = driver.session()) {
            // 记录开始时间
            long startTime = System.currentTimeMillis();
            Result res=
                    session.run("match (n:Movie) where n.movie_name contains('"+name+"') return n");

            // 记录结束时间
            long endTime = System.currentTimeMillis();


            HashMap<String,Object> response = new HashMap<>();
            response.put("time",endTime-startTime);

            return response;
        }
    }

    @GetMapping(path = "/director",produces =  MediaType.APPLICATION_JSON_VALUE)
    public HashMap<String, Object> findMovieByDirectorName(@RequestParam String name){
        //
        try (Session session = driver.session()) {
            // 记录开始时间
            long startTime = System.currentTimeMillis();
            Result res=
                    session.run("Match (d:Director{director_name:\""+name+"\"})-[r:Direct]->(m:Movie) return m.movie_name");

            // 记录结束时间
            long endTime = System.currentTimeMillis();


            HashMap<String,Object> response = new HashMap<>();
            response.put("time",endTime-startTime);

            return response;
        }
    }


    @GetMapping(path = "/actor",produces =  MediaType.APPLICATION_JSON_VALUE)
    public HashMap<String, Object> findMovieByActorName(@RequestParam String name){
        //
        try (Session session = driver.session()) {
            // 记录开始时间
            long startTime = System.currentTimeMillis();
            Result res=
                    session.run("Match (d:Actor{actor_name:\""+name+"\"})-[r:Act]->(m:Movie) return m.movie_name");

            // 记录结束时间
            long endTime = System.currentTimeMillis();


            HashMap<String,Object> response = new HashMap<>();
            response.put("time",endTime-startTime);

            return response;
        }
    }




}
