package net.nosql_bench;

import java.util.Random;

public abstract class Tester {

	static String[] words;
	static Random random = new Random();

	static {
		words = generateRandomWords(10000);
	}

	public static int randomInt(){
		return random.nextInt();
	}

	public static String randomWord(){
		return words[random.nextInt(Tester.words.length)];
	}

	public static String[] generateRandomWords(int numberOfWords) {
		String[] randomStrings = new String[numberOfWords];
		Random random = new Random();
		for (int i = 0; i < numberOfWords; i++) {
			char[] word = new char[random.nextInt(6) + 5]; // words of length 5 through 10. (1 and 2 letter words are boring.)
			for (int j = 0; j < word.length; j++) {
				word[j] = (char) ('a' + random.nextInt(26));
			}
			randomStrings[i] = new String(word);
		}
		return randomStrings;
	}
}
