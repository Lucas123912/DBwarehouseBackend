package cn.edu.tongji.dwhivebackend.Controller;

import cn.edu.tongji.dwhivebackend.DTO.MovieInfoDto;
import cn.edu.tongji.dwhivebackend.Service.HiveService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.*;

import javax.annotation.Resource;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * TODO:此处写Controller类的描述
 *
 */

@RestController
@RequestMapping("/hive")
public class HiveController {

    @Resource
    @Qualifier("jdbcTemplate")
    private JdbcTemplate jdbcTemplate;

    @Resource
    private HiveService hiveService;

    @RequestMapping("/list")
    public List<Map<String, Object>> list() {
        String sql = "select movie_title from movie";
        long startTime = System.currentTimeMillis();
        List<Map<String, Object>> list = jdbcTemplate.queryForList(sql);
        long endTime = System.currentTimeMillis();
        System.out.println(endTime - startTime);
        return list;
    }

    @RequestMapping(value = "/movie", method = RequestMethod.GET)
    public ResponseEntity<List<String>> getMovieNameListByString(
            @RequestParam(value = "movieName") String movieName
    ) {
        return new ResponseEntity<>(hiveService.getMovieNameByString(movieName), HttpStatus.OK);
    }

    @RequestMapping(value = "/director", method = RequestMethod.GET)
    public ResponseEntity<List<String>> getDirectorNameListByString(
            @RequestParam(value = "directorName") String directorName
    ) {
        return new ResponseEntity<>(hiveService.getDirectorNameByString(directorName), HttpStatus.OK);
    }

    @RequestMapping(value = "/actor", method = RequestMethod.GET)
    public ResponseEntity<List<String>> getActorNameListByString(
            @RequestParam(value = "actorName") String actorName
    ) {
        return new ResponseEntity<>(hiveService.getActorNameByStr(actorName), HttpStatus.OK);
    }

    @RequestMapping(value = "/category", method = RequestMethod.GET)
    public ResponseEntity<List<String>> getCategoryListByString(
            @RequestParam(value = "category") String category
    ) {
        return new ResponseEntity<>(hiveService.getCategoryNameByStr(category), HttpStatus.OK);
    }

    @RequestMapping(value = "/movie/director", method = RequestMethod.GET)
    public ResponseEntity<HashMap<String, Object>> getMovieDirectorByMovieAsin(
            @RequestParam(value = "movieAsin") String movieAsin,
            @RequestParam(value = "index") Integer index
    ) {
        HashMap<String, Object> res = new HashMap<>();
        res.put("index", index);
        res.put("director", hiveService.getAllDirectorsByMovieAsin(movieAsin));
        return new ResponseEntity<>(res, HttpStatus.OK);
    }


    @RequestMapping(value = "/movie/actor", method = RequestMethod.GET)
    public ResponseEntity<HashMap<String, Object>> getMovieActorByMovieAsin(
            @RequestParam(value = "movieAsin") String movieAsin,
            @RequestParam(value = "index") Integer index
    ) {
        HashMap<String, Object> res = new HashMap<>();
        res.put("index", index);
        res.put("actor", hiveService.getAllActorsByMovieAsin(movieAsin));
        return new ResponseEntity<>(res, HttpStatus.OK);
    }

    @RequestMapping(value = "/actor/cooperation", method = RequestMethod.GET)
    public ResponseEntity<HashMap<String, Object>> getActorsCooperationTime() {
        // 记录开始时间
        long startTime = System.currentTimeMillis();
        HashMap<String, Object> result = hiveService.getMaxCooperationTimeOfActors();
        long endTime = System.currentTimeMillis();
        result.put("time", endTime - startTime);
        return new ResponseEntity<>(result, HttpStatus.OK);
    }

    @RequestMapping(value = "/director/cooperation", method = RequestMethod.GET)
    public ResponseEntity<HashMap<String, Object>> getDirectorsCooperationTime() {
        //记录开始时间
        long startTime = System.currentTimeMillis();
        HashMap<String, Object> result = hiveService.getMaxCooperationTimeOfDirectors();
        long endTime = System.currentTimeMillis();
        result.put("time", endTime - startTime);
        return new ResponseEntity<>(result, HttpStatus.OK);
    }

    @RequestMapping(value = "/actor/director/cooperation", method = RequestMethod.GET)
    public ResponseEntity<HashMap<String, Object>> getActorDirectorCooperationTime() {
        long startTime = System.currentTimeMillis();
        HashMap<String, Object> result = hiveService.getMaxCooperationTimeOfActorsAndDirectors();
        long endTime = System.currentTimeMillis();
        result.put("time", endTime - startTime);
        return new ResponseEntity<>(result, HttpStatus.OK);
    }

    @RequestMapping(value = "/movie/result", method = RequestMethod.POST)
    public ResponseEntity<HashMap<String, Object>> getMovieResult(
            @RequestBody MovieInfoDto movieInfoDto
    ) {
        HashMap<String, Object> result = hiveService.getMovieResultsByMutipleRules(movieInfoDto);
        return new ResponseEntity<>(result, HttpStatus.OK);

    }

}
