package kr.sparta.movieperonalize.recommand;

import jakarta.annotation.PostConstruct;
import kr.sparta.movieperonalize.recommand.dto.MovieDto;
import kr.sparta.movieperonalize.recommand.enumtype.MovieCountry;
import kr.sparta.movieperonalize.recommand.enumtype.MovieGenre;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.util.StopWatch;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.util.UriComponents;
import org.springframework.web.util.UriComponentsBuilder;
import reactor.core.publisher.Flux;
import reactor.util.retry.Retry;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeUnit;

@Slf4j
@Service
public class RecommendService {
    @Value("${MOVIE_INFO_API_URL}")
    private String movieInfoApiUrl;

    private final WebClient.Builder webClientBuilder;
    private final RedisTemplate<String, List<MovieDto>> redisTemplate;

    private WebClient webClient;
    private UriComponents movieInfoApiUriComponent;

    public RecommendService(WebClient.Builder webClientBuilder, RedisTemplate<String, List<MovieDto>> redisTemplate) {
        this.webClientBuilder = webClientBuilder;
        this.redisTemplate = redisTemplate;
    }

    @PostConstruct
    private void init() {
        this.webClient = webClientBuilder.build();
        this.movieInfoApiUriComponent = UriComponentsBuilder
                .fromUriString(movieInfoApiUrl)
                .path("/movies")
                .build();
    }

    public Flux<MovieDto> getMoviesByGenre(MovieGenre movieGenre) {
        final String movieInfoByGenreUri = movieInfoApiUriComponent.expand(movieGenre).toUriString();

        return webClient.get()
                .uri(movieInfoByGenreUri)
                .retrieve()
                .bodyToFlux(MovieDto.class)
                .filter(movieDto -> movieDto.getGenre().contains(movieGenre.getKorean()))
                .retryWhen(Retry.backoff(3, Duration.ofMinutes(500)))
                .timeout(Duration.ofSeconds(3));
    }

    // 영화 전체 목록 API 응답을 레디스에 저장할 수 있도록 변경
    public List<MovieDto> getMovies() {
        StopWatch stopWatch = new StopWatch();
        stopWatch.start("Api - Redis 응답 속도 차이");

        // 1. Redis에서 캐시 조회
        String key = "movies";
        List<MovieDto> movies = redisTemplate.opsForValue().get(key);

        // 2. 캐시에 데이터가 존재하지 않으면
        if (movies == null) {
            // 2-1. API 호출
            String movieInfoUri = movieInfoApiUriComponent.expand().toUriString();

            movies = webClient.get()
                    .uri(movieInfoUri)
                    .retrieve()
                    .bodyToFlux(MovieDto.class)
                    .retryWhen(Retry.backoff(3, Duration.ofMinutes(500)))
                    .timeout(Duration.ofSeconds(3))
                    .collectList()
                    .block();
        }
        // 2-2 응답 값 Redis 저장
        if (movies != null) {
            redisTemplate.opsForValue().setIfAbsent(key, movies, 30, TimeUnit.SECONDS);
        }
        stopWatch.stop();
        log.info(stopWatch.prettyPrint(TimeUnit.MILLISECONDS));
        return movies;
    }

    public Flux<MovieDto> getMovieByMovieNo(Long movieNo) {
        final String movieInfoByMovieNoUri = movieInfoApiUriComponent.expand(movieNo).toUriString();

        return webClient.get()
                .uri(movieInfoByMovieNoUri)
                .retrieve()
                .bodyToFlux(MovieDto.class)
                .filter(movieDto -> movieDto.getId().equals(movieNo))
                .retryWhen(Retry.backoff(3, Duration.ofMinutes(500)))
                .timeout(Duration.ofSeconds(3));
    }

    public Flux<MovieDto> getMoviesByCountry(MovieCountry country) {
        final String movieInfoByGenreUri = movieInfoApiUriComponent.expand(country).toUriString();

        return webClient.get()
                .uri(movieInfoByGenreUri)
                .retrieve()
                .bodyToFlux(MovieDto.class)
                .filter(movieDto -> movieDto.getCountry().contains(country.getCountry()))
                .retryWhen(Retry.backoff(3, Duration.ofMinutes(500)))
                .timeout(Duration.ofSeconds(3));
    }
}