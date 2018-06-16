// This is Java code.
public class PigLatin {

    public static String pigLatin(String word) {
        char firstLetter = word.charAt(0);
        if ("aeiou".indexOf(firstLetter) != -1) return word + "ay";
        return word.substring(1) + firstLetter + "ay";
    }

    public static void main(String args[]) {
        System.out.println(pigLatin("red"));
        System.out.println(pigLatin("orange"));
    }
}
