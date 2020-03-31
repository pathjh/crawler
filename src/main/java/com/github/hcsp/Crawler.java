package com.github.hcsp;

import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.stream.Collectors;


public class Crawler {
    private CrawlerDao dao = new JdbcCrawlerDao();

    public void run() throws SQLException, IOException {

        String link;
        //从数据库中加载下一个链接，如果能加载到，则进行循环
        while ((link = dao.getNextLinkThenDelete()) != null) {
            //询问数据库当前链接是否处理过
            if (dao.isLinkProcessed(link)) {
                continue;
            }

            if (isInternetLink(link)) {
                System.out.println(link);
                Document doc = httpGetAndParseHtml(link);

                parseUrlsFromPageAndStoreIntoDatabase(doc);

                storeIntoDatabaseIfItIsNewsPage(doc, link);

                dao.updateDatabase(link, "INSERT INTO LINKS_ALREADY_PROCESSED (link) values (?)");
            }
        }
    }


    public static void main(String[] args) throws IOException, SQLException {
        new Crawler().run();
    }

    private void parseUrlsFromPageAndStoreIntoDatabase(Document doc) throws SQLException {
        for (Element aTag : doc.select("a")) {
            String href = aTag.attr("href");
            if (href.startsWith("//")) {
                href = "https:" + href;
            }
            if (!href.toLowerCase().startsWith("javascript")) {
                dao.updateDatabase(href, "INSERT INTO LINKS_TO_BE_PROCESSED (link) values (?)");
            }

        }
    }


    private void storeIntoDatabaseIfItIsNewsPage(Document doc, String link) throws SQLException {
        ArrayList<Element> articalTags = doc.select("article");
        if (!articalTags.isEmpty()) {
            for (Element articalTag : articalTags
            ) {
                String title = articalTags.get(0).child(0).text();
                String content = articalTag.select("p").stream().map(Element::text).collect(Collectors.joining("\n"));
                dao.insertNewsIntoDatabase(link, title, content);

            }
        }
    }

    private static Document httpGetAndParseHtml(String link) throws IOException {
        CloseableHttpClient httpclient = HttpClients.createDefault();
//        System.out.println(link);
        HttpGet httpGet = new HttpGet(link);
        httpGet.addHeader("user-agent", " Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.132 Safari/537.36");
        try (CloseableHttpResponse response1 = httpclient.execute(httpGet)) {
//            System.out.println(response1.getStatusLine());
            HttpEntity entity1 = response1.getEntity();
            String html = EntityUtils.toString(entity1);
            return Jsoup.parse(html);
        }
    }

    //我们只关心news,sina的，我们要排除登录界面
    private static boolean isInternetLink(String link) {
        return isNotLoginPage(link) && (isNewsPageLink(link) || isIndexPage(link)) && isNormalLink(link);
    }

    private static boolean isNotLoginPage(String link) {
        return !link.contains("passport.sina.cn");
    }

    private static boolean isNewsPageLink(String link) {
        return link.contains("news.sina.cn");
    }

    private static boolean isIndexPage(String link) {
        return "https://sina.cn".equals(link);
    }

    private static boolean isNormalLink(String link) {
        return !link.contains("\\/");
    }

}
