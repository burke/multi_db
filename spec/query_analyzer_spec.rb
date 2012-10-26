require './lib/multi_db/query_analyzer'

describe MultiDb::QueryAnalyzer do

  describe "Table detection" do
    def self.q(sql, *tables)
      specify { subject.tables(sql).should == tables }
    end

    #simple selects
    q("SELECT * FROM `products`", "products")
    q("SELECT * FROM products", "products")
    q("SELECT * from products", "products")
    q("select * from   `products`", "products")

    #insert
    q("insert into products (a,b,c) values(1,2,3)", "products")

    #update
    q("update products set id=1", "products")

    #joins
    q("select * from products left join images on products.image_id = images.id", "products", "images")

    #subselect
    q("select * from products where id in (select product_id from images)", "products", "images")

    #multiple tables
    q("select * from products, images", "products", "images")
    q("select * from a,b,c,d,e,f", *%w(a b c d e f))
    q("select * from a,`b`,c,`d` ,e,f", *%w(a b c d e f))
  end


  describe "Session stuff" do

    CURRENT_TIME = 1000

    before do
      now = mock(to_i: CURRENT_TIME)
      Time.stub(now: now)
    end

    it "doesn't require sticky on a fresh read" do
      session = {}
      subject.query_requires_sticky?(session, "select * from products").should be_false
    end

    it "requires sticky when specified" do
      session = {sticky_expires: CURRENT_TIME+1, sticky_tables: {"products" => CURRENT_TIME + 1}}
      subject.query_requires_sticky?(session, "select * from products").should be_true
    end

    # this is an invalid state, but demonstrates that a performance optimization has not been removed.
    it "doesn't require sticky when the sticky_expires is passed, even if the table somehow isn't" do
      session = {sticky_expires: CURRENT_TIME-1, sticky_tables: {"products" => CURRENT_TIME + 1}}
      subject.query_requires_sticky?(session, "select * from products").should be_false
    end

    it "marks tables as sticky." do
      session = subject.mark_sticky_tables_in_session({}, "DELETE FROM products, images", 1)
      exp = {
        sticky_expires: CURRENT_TIME+1,
        sticky_tables: {
          "products" => CURRENT_TIME+1,
          "images" => CURRENT_TIME+1,
        }
      }
      session.should == exp
    end

    it "adds stickies to an existing session and purges expired stickies" do
      prev = {
        sticky_expires: CURRENT_TIME-100,
        sticky_tables: {
          "baz" => CURRENT_TIME+2,
          "foobars" => CURRENT_TIME-100,
          "products" => CURRENT_TIME-100
        }
      }
      session = subject.mark_sticky_tables_in_session(prev, "DELETE FROM products, images", 1)
      exp = {
        sticky_expires: CURRENT_TIME+1,
        sticky_tables: {
          "baz" => CURRENT_TIME+2,
          "products" => CURRENT_TIME+1,
          "images" => CURRENT_TIME+1,
        }
      }
      session.should == exp
      subject.query_requires_sticky?(session, "SELECT * FROM products").should be_true
      subject.query_requires_sticky?(session, "SELECT * FROM foobars").should be_false
    end

  end

end
