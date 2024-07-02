import java.sql.*;
import java.util.HashMap;
import java.util.Map;

public class Metadata {
    private static final String url = "jdbc:mysql://localhost:3306/causw";
    private static final String username = "root";
    private static final String password = "0530";

    public static void main(String[] args) {
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            System.out.println("MySQL JDBC 드라이버를 찾을 수 없습니다.");
            e.printStackTrace();
            return;
        }
        try (Connection connection = DriverManager.getConnection(url, username, password)) {
            System.out.println("MySQL 데이터베이스에 연결되었습니다.");

            try (Statement statement = connection.createStatement()) {
                String queryRelation = "SELECT * FROM Relation_metadata";
                try (ResultSet resultSet = statement.executeQuery(queryRelation)) {
                    if (!resultSet.isBeforeFirst()) {
                        System.out.println("테이블이 비어있습니다.");
                    } else {
                        while (resultSet.next()) {
                            String relation_name = resultSet.getString("relation_name");
                            int number_of_attributes = resultSet.getInt("number_of_attributes");
                            String storage_organization = resultSet.getString("storage_organization");
                            String location = resultSet.getString("location");
                            System.out.println("relation_name: " + relation_name + ", number_of_attributes: " + number_of_attributes + ", storage_organization: " + storage_organization + ", location: " + location);
                        }
                    }
                }
                String queryAttribute = "SELECT * FROM Attribute_metadata";
                try (ResultSet resultSet = statement.executeQuery(queryAttribute)) {
                    if (!resultSet.isBeforeFirst()) {
                        System.out.println("테이블이 비어있습니다.");
                    } else {
                        while (resultSet.next()) {
                            String relation_name = resultSet.getString("relation_name");
                            String attribute_name = resultSet.getString("attribute_name");
                            String domain_type = resultSet.getString("domain_type");
                            int position = resultSet.getInt("position");
                            int length= resultSet.getInt("length");
                            System.out.println("relation_name: " + relation_name + ", attribute_name: " + attribute_name + ", domain_type: " + domain_type + ", position: " + position + ", length: " + length);
                        }
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("MySQL 데이터베이스에 연결할 수 없습니다.");
            e.printStackTrace();
        }
    }
    public static void saveRelationMetadata(String relation_name, int number_of_attributes, String location) {
        try (Connection connection = DriverManager.getConnection(url, username, password)) {
            String relation_query = "INSERT INTO Relation_metadata (relation_name, number_of_attributes, storage_organization, location) VALUES (?, ?, ?, ?)";
            try (PreparedStatement preparedStatement = connection.prepareStatement(relation_query)) {
                preparedStatement.setString(1, relation_name);
                preparedStatement.setInt(2, number_of_attributes);
                preparedStatement.setString(3, "heap");
                preparedStatement.setString(4, location);
                preparedStatement.executeUpdate();
                System.out.println("Relation_metadata 테이블에 데이터가 저장되었습니다.");
            }
        } catch (SQLException e) {
            System.out.println("MySQL 데이터베이스에 연결할 수 없습니다.");
            e.printStackTrace();
        }
    }
    public static void saveAttributeMetadata(String relation_name, String attribute_name, String domain_type, int position, int length) {
        try (Connection connection = DriverManager.getConnection(url, username, password)) {
            String attribute_query = "INSERT INTO Attribute_metadata (relation_name, attribute_name, domain_type, position, length) VALUES (?,?,?,?,?)";
            try (PreparedStatement preparedStatement = connection.prepareStatement(attribute_query)) {
                preparedStatement.setString(1, relation_name);
                preparedStatement.setString(2, attribute_name);
                preparedStatement.setString(3, domain_type);
                preparedStatement.setInt(4, position);
                preparedStatement.setInt(5, length);
                preparedStatement.executeUpdate();
                System.out.println("Attribute_metadata 테이블에 데이터가 저장되었습니다.");
            }
        } catch (SQLException e) {
            System.out.println("MySQL 데이터베이스에 연결할 수 없습니다.");
            e.printStackTrace();
        }
    }

    public static boolean searchRelationMetadata(String relation_name) {
        boolean table_exists = false;
        try (Connection connection = DriverManager.getConnection(url, username, password)) {
            String query = "SELECT COUNT(*) AS count FROM Relation_Metadata WHERE relation_name = ?";
            try (PreparedStatement preparedStatement = connection.prepareStatement(query)) {
                preparedStatement.setString(1, relation_name);
                try (ResultSet resultSet = preparedStatement.executeQuery()) {
                    if (resultSet.next()) {
                        int count = resultSet.getInt("count");
                        if (count > 0) {
                            table_exists = true;
                        }
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("찾을 수 없는 테이블");
            e.printStackTrace();
        }
        return table_exists;
    }

    public static int getLength(String relation_name, String attribute_name) {
        int length = 30;
        try (Connection connection = DriverManager.getConnection(url, username, password)) {
            String query = "SELECT length FROM Attribute_Metadata WHERE relation_name = ? AND attribute_name = ?";
            try (PreparedStatement preparedStatement = connection.prepareStatement((query))) {
                preparedStatement.setString(1, relation_name);
                preparedStatement.setString(2, attribute_name);
                try (ResultSet resultSet = preparedStatement.executeQuery()) {
                    if (resultSet.next()) {
                        length = resultSet.getInt("length");
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("찾을 수 없는 정보");
            e.printStackTrace();
        }
        return length;
    }

     public static Map<String, Integer> getPosition(String relation_name) {
        Map<String, Integer> position_map = new HashMap<>();
        try (Connection connection = DriverManager.getConnection(url, username, password)) {
            String query = "SELECT attribute_name, position FROM Attribute_Metadata WHERE relation_name = ?";
            try (PreparedStatement preparedStatement = connection.prepareStatement(query)) {
                preparedStatement.setString(1, relation_name);
                try (ResultSet resultSet = preparedStatement.executeQuery()) {
                    while (resultSet.next()) {
                        String attribute_name = resultSet.getString("attribute_name");
                        int position = resultSet.getInt("position");
                        position_map.put(attribute_name, position);
                    }
                }
            }
        } catch (SQLException e) {
            System.out.println("속성의 위치를 가져오는 중 오류가 발생했습니다.");
            e.printStackTrace();
        }
        return position_map;
    }

}
