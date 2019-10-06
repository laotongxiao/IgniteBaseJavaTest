package com.peppapigdaddy.ignite.example;

public class City {
    private String cityName;
    private String provinceName;
    private long population;

    public City(String cityName, String provinceName, long population) {
        this.cityName = cityName;
        this.provinceName = provinceName;
        this.population = population;
    }

    public String getCityName() {
        return cityName;
    }

    public void setCityName(String cityName) {
        this.cityName = cityName;
    }

    public String getProvinceName() {
        return provinceName;
    }

    public void setProvinceName(String provinceName) {
        this.provinceName = provinceName;
    }

    public long getPopulation() {
        return population;
    }

    public void setPopulation(long population) {
        this.population = population;
    }

    @Override
    public String toString() {
        return "City{" +
                "cityName='" + cityName + '\'' +
                ", provinceName='" + provinceName + '\'' +
                ", population=" + population +
                '}';
    }
}
