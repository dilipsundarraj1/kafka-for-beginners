package com.learnjava.launcher;

import com.learnjava.producer.MessageProducer;
import com.sun.tools.javac.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Scanner;
import java.util.StringTokenizer;

import static com.learnjava.producer.MessageProducer.buildProducerProperties;

public class CommandLineLauncher {
    private static final Logger logger = LoggerFactory.getLogger(MessageProducer.class);

    public static String commandLineStartLogo(){

        return " __      _____________.____   _________  ________      _____  ___________                       \n" +
                "/  \\    /  \\_   _____/|    |  \\_   ___ \\ \\_____  \\    /     \\ \\_   _____/                       \n" +
                "\\   \\/\\/   /|    __)_ |    |  /    \\  \\/  /   |   \\  /  \\ /  \\ |    __)_                        \n" +
                " \\        / |        \\|    |__\\     \\____/    |    \\/    Y    \\|        \\                       \n" +
                "  \\__/\\  / /_______  /|_______ \\______  /\\_______  /\\____|__  /_______  /                       \n" +
                "       \\/          \\/         \\/      \\/         \\/         \\/        \\/                        \n" +
                "  __                                                                                            \n" +
                "_/  |_  ____                                                                                    \n" +
                "\\   __\\/  _ \\                                                                                   \n" +
                " |  | (  <_> )                                                                                  \n" +
                " |__|  \\____/                                                                                   \n" +
                "                                                                                                \n" +
                "_________                                           .___.____    .__                            \n" +
                "\\_   ___ \\  ____   _____   _____ _____    ____    __| _/|    |   |__| ____   ____               \n" +
                "/    \\  \\/ /  _ \\ /     \\ /     \\\\__  \\  /    \\  / __ | |    |   |  |/    \\_/ __ \\              \n" +
                "\\     \\___(  <_> )  Y Y  \\  Y Y  \\/ __ \\|   |  \\/ /_/ | |    |___|  |   |  \\  ___/              \n" +
                " \\______  /\\____/|__|_|  /__|_|  (____  /___|  /\\____ | |_______ \\__|___|  /\\___  >             \n" +
                "        \\/             \\/      \\/     \\/     \\/      \\/         \\/       \\/     \\/              \n" +
                " ____  __.       _____ __             __________                   .___                         \n" +
                "|    |/ _|____ _/ ____\\  | _______    \\______   \\_______  ____   __| _/_ __   ____  ___________ \n" +
                "|      < \\__  \\\\   __\\|  |/ /\\__  \\    |     ___/\\_  __ \\/  _ \\ / __ |  |  \\_/ ___\\/ __ \\_  __ \\\n" +
                "|    |  \\ / __ \\|  |  |    <  / __ \\_  |    |     |  | \\(  <_> ) /_/ |  |  /\\  \\__\\  ___/|  | \\/\n" +
                "|____|__ (____  /__|  |__|_ \\(____  /  |____|     |__|   \\____/\\____ |____/  \\___  >___  >__|   \n" +
                "        \\/    \\/           \\/     \\/                                \\/           \\/    \\/       ";
    }

    public static String BYE(){

        return "_______________.___.___________\n" +
                "\\______   \\__  |   |\\_   _____/\n" +
                " |    |  _//   |   | |    __)_ \n" +
                " |    |   \\\\____   | |        \\\n" +
                " |______  // ______|/_______  /\n" +
                "        \\/ \\/               \\/ ";
    }

    public static void launchCommandLine(){
        boolean cliUp = true;
        while (cliUp){
            Scanner scanner = new Scanner(System.in);
            userOptions();
            String option = scanner.next();
            logger.info("Selected Option is : {} ", option);
            switch (option) {
                case "1":
                case "2":
                    acceptMessageFromUser(option);
                    break;
                case "3":
                    cliUp = false;
                    break;
                default:
                    break;

            }
        }
    }

    public static void userOptions(){
        List<String> userInputList = List.of("1: Kafka Producer", "2: Kafka Producer With Key", "3: Exit");
        System.out.println("Please select one of the below options:");
        for(String userInput: userInputList ){
            System.out.println(userInput);
        }
    }
    public static MessageProducer init(){

        Map<String, String> producerProps = buildProducerProperties();
        MessageProducer messageProducer = new MessageProducer(producerProps);
        return messageProducer;
    }

    public static void publishMessage(MessageProducer messageProducer, String input){
        StringTokenizer stringTokenizer = new StringTokenizer(input, "-");
        Integer noOfTokens = stringTokenizer.countTokens();
        switch (noOfTokens){
            case 1:
                messageProducer.publishMessageSync(null,stringTokenizer.nextToken());
                break;
            case 2:
                messageProducer.publishMessageSync(stringTokenizer.nextToken(),stringTokenizer.nextToken());
                break;
            default:
                break;
        }
    }

    public static void acceptMessageFromUser(String option){
        MessageProducer messageProducer = init();
        Scanner scanner = new Scanner(System.in);
        boolean flag= true;
        while (flag){
            System.out.println("Please Enter a Message to produce to Kafka:");
            String input = scanner.nextLine();
            logger.info("Entered message is {}", input);
            if(input.equals("00")) {
                flag = false;
            }else {
                publishMessage(messageProducer, input);
            }
        }
        logger.info("Exiting from Option 1");
    }

    public static void main(String[] args) {

        System.out.println(commandLineStartLogo());
        launchCommandLine();
        System.out.println(BYE());


    }
}
