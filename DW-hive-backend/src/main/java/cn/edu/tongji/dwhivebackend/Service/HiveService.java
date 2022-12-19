package cn.edu.tongji.dwhivebackend.Service;

import cn.edu.tongji.dwhivebackend.DTO.MovieInfoDto;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;

/**
 * TODO:此处写HiveService类的描述
 *
 * @author shotray
 * @since 2021/12/17 16:41
 */

@Service
public interface HiveService {
    List<String> getMovieNameByString(String movieString);

    List<String> getDirectorNameByString(String directorString);

    List<String> getActorNameByStr(String actorString);

    List<String> getCategoryNameByStr(String category);

    List<String> getAllDirectorsByMovieAsin(String movieAsin);


    List<String> getAllActorsByMovieAsin(String movieAsin);

    HashMap<String,Object> getMaxCooperationTimeOfActors();

    HashMap<String,Object> getMaxCooperationTimeOfDirectors();

    HashMap<String,Object> getMaxCooperationTimeOfActorsAndDirectors();

    HashMap<String,Object> getMovieResultsByMutipleRules(MovieInfoDto movieInfoDto);

}

