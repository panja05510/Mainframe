package com.citizens.mainframe;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class MainframeApplication {

	public static void main(String[] args) {
		SpringApplication.run(MainframeApplication.class, args);
		System.out.println(
				"**************************************************************************** \n"
				+ "*                          MAINFRAME                                     *\n"
			    +"**************************************************************************** \n"
				);
	}

}
