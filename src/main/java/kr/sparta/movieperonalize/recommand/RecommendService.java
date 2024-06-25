package kr.sparta.movieperonalize.recommand;

import jakarta.annotation.PostConstruct;
import kr.sparta.movieperonalize.recommand.dto.MovieDto;
import kr.sparta.movieperonalize.recommand.enumtype.MovieCountry;
import kr.sparta.movieperonalize.recommand.enumtype.MovieGenre;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.util.UriComponents;
import org.springframework.web.util.UriComponentsBuilder;
import reactor.core.publisher.Flux;
import reactor.util.retry.Retry;

import java.time.Duration;

@Slf4j
@Service
public class RecommendService {
    @Value("${MOVIE_INFO_API_URL}")
    private String movieInfoApiUrl;

    private final WebClient.Builder webClientBuilder;

    private WebClient webClient;
    private UriComponents movieInfoApiUriComponent;

    public RecommendService(WebClient.Builder webClientBuilder) {
        this.webClientBuilder = webClientBuilder;
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

    public Flux<MovieDto> getMovies() {
        final String movieInfoUri = movieInfoApiUriComponent.expand().toUriString();

        return webClient.get()
                .uri(movieInfoUri)
                .retrieve()
                .bodyToFlux(MovieDto.class)
                .retryWhen(Retry.backoff(3, Duration.ofMinutes(500)))
                .timeout(Duration.ofSeconds(3));
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