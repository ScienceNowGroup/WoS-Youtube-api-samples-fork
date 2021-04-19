package com.google.api.services.samples.youtube.cmdline.analytics;

import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.samples.youtube.cmdline.Auth;
import com.google.api.services.youtube.YouTube;
import com.google.api.services.youtube.model.*;
import com.google.api.services.youtubeAnalytics.v2.YouTubeAnalytics;
import com.google.api.services.youtubeAnalytics.v2.model.QueryResponse;
import com.google.api.services.youtubeAnalytics.v2.model.ResultTableColumnHeader;
import com.google.common.collect.Lists;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringEscapeUtils;

import java.io.*;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.Comparator;
import java.util.List;

/**
 * This example uses the YouTube Data and YouTube Analytics APIs to retrieve
 * YouTube Analytics data. It also uses OAuth 2.0 for authorization.
 *
 * @author Christoph Schwab-Ganser and Jeremy Walker
 */
public class YouTubeAnalyticsReports {

    /**
     * Define a global instance of the HTTP transport.
     */
    private static final HttpTransport HTTP_TRANSPORT = new NetHttpTransport();

    /**
     * Define a global instance of the JSON factory.
     */
    private static final JsonFactory JSON_FACTORY = new JacksonFactory();

    /**
     * Define a global instance of a Youtube object, which will be used
     * to make YouTube Data API requests.
     */
    private static YouTube youtube;

    /**
     * Define a global instance of a YoutubeAnalytics object, which will be
     * used to make YouTube Analytics API requests.
     */
    private static YouTubeAnalytics analytics;

    /**
     * This code authorizes the user, uses the YouTube Data API to retrieve
     * information about the user's YouTube channel, and then fetches and
     * prints statistics for the user's channel using the YouTube Analytics API.
     *
     * @param args command line args (not used).
     */
    public static void main(String[] args) {

        // These scopes are required to access information about the
        // authenticated user's YouTube channel as well as Analytics
        // data for that channel.
        List<String> scopes = Lists.newArrayList(
                "https://www.googleapis.com/auth/youtube.readonly",
                "https://www.googleapis.com/auth/youtubepartner",
                "https://www.googleapis.com/auth/yt-analytics.readonly",
                "https://www.googleapis.com/auth/yt-analytics-monetary.readonly"
        );
        PrintStream fileWriter = null;
        try {
            // Authorize the request.
            Credential credential = Auth.authorize(scopes, "analyticsreports");

            // This object is used to make YouTube Data API requests.
            youtube = new YouTube.Builder(HTTP_TRANSPORT, JSON_FACTORY, credential)
                    .setApplicationName("youtube-analytics-api-report-example")
                    .build();

            // This object is used to make YouTube Analytics API requests.
            analytics = new YouTubeAnalytics.Builder(HTTP_TRANSPORT, JSON_FACTORY, credential)
                    .setApplicationName("youtube-analytics-api-report-example")
                    .build();

            final int currentYear = LocalDate.now().getYear();
            final String currency = "GBP";
            for (int year = 2014; year <= currentYear; year++) {
                File REPORT_FILE = new File("YouTubeRevenueReportWoS" + year + ".csv");
                PrintStream consoleWriter = System.out;
                if (REPORT_FILE.exists()) {
                    FileUtils.forceDelete(REPORT_FILE);
                }

                fileWriter = new PrintStream(REPORT_FILE, "UTF-8");
                // Construct a request to retrieve the current user's channel ID.
                Channel defaultChannel = getMainChannel();
                consoleWriter.println("Default Channel: " + defaultChannel.getSnippet().getTitle() +
                        " ( " + defaultChannel.getId() + " )\n");

                List<Playlist> playlists = getPlaylists(defaultChannel);
                playlists.sort(Comparator.comparing(o -> o.getSnippet().getTitle()));

                for (Playlist playlist : playlists) {
                    List<PlaylistItem> playlistItems = getPlaylistItems(playlist);
                    List<String> videoIds = extractVideoIds(playlistItems);
                    String playlistInfo = "playlist: " + playlist.getSnippet().getTitle() + ", videos: " + videoIds.size();
                    System.out.println(playlistInfo);
                    if (videoIds.isEmpty()) {
                        System.out.println("ignoring empty playlist: " + playlistInfo);
                        continue;
                    }
                    QueryResponse report = revenueReport(analytics, defaultChannel, videoIds, year, currency);
                    printData(consoleWriter, report);
                    writePlaylistInfoToReport(fileWriter, playlistInfo, report, currency);
                }
            }
        } catch (IOException e) {
            System.err.println("IOException: " + e.getMessage());
            e.printStackTrace();
        } finally {
            if (fileWriter != null) {
                fileWriter.close();
            }
        }
    }

    private static void writePlaylistInfoToReport(PrintStream fileWriter, String playlistInfo, QueryResponse report, String currency) throws IOException {
        StringBuilder sb = new StringBuilder("\r\n");
        appendCsvValue(sb, playlistInfo);
        printLn(fileWriter, sb);
        appendCsvValue(sb, "month");
        appendCsvValue(sb, "revenue " + currency);
        printLn(fileWriter, sb);
        for (List<Object> row : report.getRows()) {
            appendCsvValue(sb, row.get(0));
            appendCsvValue(sb, row.get(1));
            printLn(fileWriter, sb);
        }
    }
    private static void printLn(PrintStream fileWriter, StringBuilder sb) {
        fileWriter.println(sb);
        sb.setLength(0);
    }

    private static void appendCsvValue(StringBuilder sb, Object value) {
        String strValue = (value == null)? "" : value.toString();
        sb.append(StringEscapeUtils.escapeCsv(strValue)).append(",");
    }

    private static Channel getMainChannel() throws IOException {
        YouTube.Channels.List channelRequest = youtube.channels().list("id,snippet");
        channelRequest.setMine(true);
        channelRequest.setFields("items(id,snippet/title)");
        ChannelListResponse channels = channelRequest.execute();

        // List channels associated with the user.
        List<Channel> listOfChannels = channels.getItems();
        // The user's default channel is the first item in the list.
        return listOfChannels.get(0);
    }

    private static List<String> extractVideoIds(List<PlaylistItem> playlistItems) {
        List<String> ids = Lists.newArrayList();
        for (PlaylistItem playListItem : playlistItems) {
            ids.add(playListItem.getContentDetails().getVideoId());
        }
        return ids;
    }

    private static List<PlaylistItem> getPlaylistItems(Playlist playlist) throws IOException {
        YouTube.PlaylistItems.List playlistItemsRequest = youtube.playlistItems()
                .list("snippet,contentDetails")
                .setFields("items(contentDetails/videoId,id,snippet/title),nextPageToken")
                .setMaxResults(50L)
                .setPlaylistId(playlist.getId());

        List<PlaylistItem> list = Lists.newArrayList();
        String nextPageToken = "";
        while (nextPageToken != null) {
            if (list.size() > 0) {
                playlistItemsRequest.setPageToken(nextPageToken);
            }
            PlaylistItemListResponse playlistsItemsResponse = playlistItemsRequest.execute();
            list.addAll(playlistsItemsResponse.getItems());
            nextPageToken = playlistsItemsResponse.getNextPageToken();
        }
        return list;
    }

    private static List<Playlist> getPlaylists(Channel channel) throws IOException {
        YouTube.Playlists.List playlistRequest = youtube.playlists().list("id,snippet");
        playlistRequest.setChannelId(channel.getId());
        playlistRequest.setMaxResults(50L);
        playlistRequest.setFields("items(id,snippet/title),nextPageToken");

        List<Playlist> list = Lists.newArrayList();
        String nextPageToken = "";
        while (nextPageToken != null) {
            if (list.size() > 0) {
                playlistRequest.setPageToken(nextPageToken);
            }
            PlaylistListResponse playlistsResponse = playlistRequest.execute();
            list.addAll(playlistsResponse.getItems());
            nextPageToken = playlistsResponse.getNextPageToken();
        }
        return list;
    }

    private static QueryResponse revenueReport(YouTubeAnalytics analytics, Channel channel,
                                               List<String> ids, int year, String currency) throws IOException {

        return analytics.reports()
                .query ()
                .setIds ("channel==" + channel.getId())
                .setMetrics ("estimatedRevenue")
                .setCurrency(currency)
                .setStartDate (year + "-01-01")
                .setEndDate (year + "-12-01")
                .setDimensions("month")
                .setSort("month")
                .setFilters ("video==" + String.join(",", ids))
                .execute();
    }

/*    private static QueryResponse executeViewsOverTimeQuery(YouTubeAnalytics analytics,
                                                           String id) throws IOException {

        return analytics.reports()
                .query ()
                .setIds ("channel==" + id)
                .setMetrics ("estimatedRevenue")
                .setStartDate ("2019-01-01")
                .setEndDate ("2019-12-01")
                .setDimensions("month")
                .setSort("month")
                .execute();
    }
*/
    /**
     * Retrieve the channel's 10 most viewed videos in descending order.
     *
     * @param analytics the analytics service object used to access the API.
     * @param id        the string id from which to retrieve data.
     * @return the response from the API.
     * @throws IOException if an API error occurred.
     */
    private static QueryResponse executeTopVideosQuery(YouTubeAnalytics analytics,
                                                       String id) throws IOException {

        return analytics.reports()
                .query ()
                .setIds ("channel==" + id)
                .setMetrics ("views,subscribersGained,subscribersLost")
                .setStartDate ("2019-01-01")
                .setEndDate ("2019-12-31")
                .setDimensions("video")
                .setSort("-views")
                .setMaxResults(10)
                .execute();
    }

    /**
     * Retrieve the demographics report for the channel.
     *
     * @param analytics the analytics service object used to access the API.
     * @param id        the string id from which to retrieve data.
     * @return the response from the API.
     * @throws IOException if an API error occurred.
     */
    private static QueryResponse executeDemographicsQuery(YouTubeAnalytics analytics,
                                                          String id) throws IOException {
        return analytics.reports()
                .query ()
                .setIds ("channel==" + id)
                .setMetrics ("viewerPercentage")
                .setStartDate ("2019-01-01")
                .setEndDate ("2019-12-31")
                .setDimensions("ageGroup,gender")
                .setSort("-viewerPercentage")
                .execute();
    }

    /**
     * Prints the API response. The channel name is printed along with
     * each column name and all the data in the rows.
     *
     * @param writer  stream to output to
     * @param results data returned from the API.
     */
    private static void printData(PrintStream writer, QueryResponse results) {
        if (results.getRows() == null || results.getRows().isEmpty()) {
            writer.println("No results Found.");
        } else {

            // Print column headers.
            for (ResultTableColumnHeader header : results.getColumnHeaders()) {
                writer.printf("%30s", header.getName());
            }
            writer.println();

            // Print actual data.
            for (List<Object> row : results.getRows()) {
                for (int colNum = 0; colNum < results.getColumnHeaders().size(); colNum++) {
                    ResultTableColumnHeader header = results.getColumnHeaders().get(colNum);
                    Object column = row.get(colNum);
                    if ("INTEGER".equals(header.getUnknownKeys().get("dataType"))) {
                        long l = ((BigDecimal) column).longValue();
                        writer.printf("%30d", l);
                    } else if ("FLOAT".equals(header.getUnknownKeys().get("dataType"))) {
                        writer.printf("%30f", column);
                    } else if ("STRING".equals(header.getUnknownKeys().get("dataType"))) {
                        writer.printf("%30s", column);
                    } else {
                        // default output.
                        writer.printf("%30s", column);
                    }
                }
                writer.println();
            }
            writer.println();
        }
    }

}
