using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Enyim.Caching.SampleWebApp.Models;

namespace Enyim.Caching.SampleWebApp.Services
{
    public class BlogPostService : IBlogPostService
    {
        public async ValueTask<Dictionary<string, List<BlogPost>>> GetRecent(int itemCount)
        {
            var dict = new Dictionary<string, List<BlogPost>>();
            var posts = new List<BlogPost>
            {
                new BlogPost
                {
                    Title = "Hello World",
                    Body = "EnyimCachingCore"
                }
            };

            dict.Add(DateTime.Today.ToString("yyyy-MM-dd"), posts);

            return dict;
        }
    }
}
