package kr.sparta.movieperonalize.recommand.enumtype;

import lombok.Getter;

@Getter
public enum MovieCountry {
    KOREA("대한민국"),
    USA("미국"),
    JAPAN("일본");

    private String country;
    MovieCountry(String country) {
        this.country = country;
    }
}
