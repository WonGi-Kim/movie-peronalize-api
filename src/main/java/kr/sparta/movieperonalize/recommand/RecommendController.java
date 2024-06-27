package kr.sparta.movieperonalize.recommand;

import kr.sparta.movieperonalize.recommand.dto.MovieDto;
import kr.sparta.movieperonalize.recommand.enumtype.MovieCountry;
import kr.sparta.movieperonalize.recommand.enumtype.MovieGenre;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;

import java.util.List;

@RestController
@RequestMapping("/v1")
public class RecommendController {
    private final RecommendService recommendService;

    public RecommendController(RecommendService recommendService) {
        this.recommendService = recommendService;
    }

    @GetMapping("/movies/genre/{genre}")
    public Flux<MovieDto> getMoviesByGenre(@PathVariable MovieGenre genre) {
        return recommendService.getMoviesByGenre(genre);
    }

    @GetMapping("/movies")
    public List<MovieDto> getMovies() {
        return recommendService.getMovies();
    }

    @GetMapping("/movies/{movieNo}")
    public Flux<MovieDto> getMoviesByMovieNo(@PathVariable Long movieNo) {
        return recommendService.getMovieByMovieNo(movieNo);
    }

    @GetMapping("/movies/country/{country}")
    public Flux<MovieDto> getMoviesByCountry(@PathVariable MovieCountry country) {
        return recommendService.getMoviesByCountry(country);
    }
}
