require 'selenium-webdriver'
require 'webdrivers'
require 'capybara'
require 'capybara/dsl'
Capybara.default_driver = :selenium_chrome
Capybara.app_host = 'http://google.com'

describe 'Google' do
  include Capybara::DSL
  it 'contains button "Im feeling lucky"' do
    visit '/'

    expect(page).to have_button 'იღბალს მივენდობი'
  end
end
