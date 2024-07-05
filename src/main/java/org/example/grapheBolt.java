package org.example;

import javafx.application.Application;
import javafx.scene.Scene;
import javafx.scene.chart.LineChart;
import javafx.scene.chart.NumberAxis;
import javafx.scene.chart.XYChart;
import javafx.stage.Stage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.io.FileReader;
import java.io.BufferedReader;

public class grapheBolt extends Application {
    @Override
    public void start(Stage Stage) {
        stage.setTitle("Aauction Prices");

        final NumberAxis xAxis = new NumberAxis();
        final NumberAxis yAxis = new NumberAxis();
        xAxis.setLabel("source");
        yAxis.setLabel("personne ");

        final LineChart<Number,Number> lineChart = new LineChart<>(xAxis, yAxis);
        lineChart.setTitle("GrapheMOBolt");

        List<MO> MOlist = readDataFromFile("/home/ji/storm/examples/twitter/src/main/resources/mood_origine.txt");

        XYChart.Series<Number,Number> android1 = new XYChart.Series<>();
        android1.setName("Android1");
        XYChart.Series<Number,Number> android2 = new XYChart.Series<>();
        android1.setName("Android2");
        XYChart.Series<Number,Number> android3 = new XYChart.Series<>();
        android1.setName("Android3");
        XYChart.Series<Number,Number> ios1 = new XYChart.Series<>();
        android1.setName("ios1");
        XYChart.Series<Number,Number> ios2 = new XYChart.Series<>();
        android1.setName("ios2");
        XYChart.Series<Number,Number> ios3 = new XYChart.Series<>();
        android1.setName("ios3");
        XYChart.Series<Number,Number> web1 = new XYChart.Series<>();
        android1.setName("web1");
        XYChart.Series<Number,Number> web2 = new XYChart.Series<>();
        android1.setName("web2");
        XYChart.Series<Number,Number> web3 = new XYChart.Series<>();
        android1.setName("web3");

        int indexandroid1=0;
        int indexandroid2=0;
        int indexandroid3=0;
        for (MO mo : MOlist){
            if( mo.source == "Twitter for Android" && mo.mood == "neutral") android1.getData().add(new XYChart.Data<>(indexandroid1,));

        }